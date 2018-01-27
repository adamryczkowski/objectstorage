
# nocov start
.onLoad	<-	function(libname,	pkgname)	{
  op	<-	options()
  op.objectstorage	<-	list(
    lock.extension	=	'.lock',
    index.extension = '.rdx',
    default_archive.extension = '.rda', #Used only for the main archive for small objects
    reserved_attr_for_hash ='..hash',
    prefix_for_automatic_dedicated_archive_names='_',
    default_archive.name = '_default_archive.rda', #Name of the folder to save objects if more than one
    default.lock.time = 3600, #1 hour
    tune.threshold_objsize_for_dedicated_archive = 20000 #Results from `studium_save` multiplied by 4.
  )
  toset	<-	!(names(op.objectstorage)	%in%	names(op))
  if(any(toset))	options(op.objectstorage[toset])
  invisible()
}
# nocov end

#' Makes sure there is a objectstorage on the specified path.
#'
#' @param storagepath Path where the storage should be created
#' @return Returns updated metadata on success, and \code{NULL} on failure.
#' @export

create_objectstorage<-function(storagepath) {
  path<-get_runtime_index_path(storagepath)
  if(!file.exists(path)) {
    idx<-data.table(objectnames=character(0), digest=character(0),
                    size=numeric(0), archive_filename=character(0),
                    single_object=logical(0))
    path<-get_runtime_index_path(storagepath)
    saveRDS(object = idx, file = path)
  } else {
    idx<-list_runtime_objects(storagepath)
  }
  return(idx)
}

#' Lists all the runtime objects cotained in the task with path path.
#'
#' @param storagepath Path with the storage.
#' @return Returns `data.frame` with the following columns:
#' \describe{
#' \item{\strong{objectname}}{Name of the stored object. This is a primary key.}
#' \item{\strong{digest}}{String with the digest of the object.}
#' \item{\strong{size}}{Numeric value with the size of the stored object.}
#' \item{\strong{archive_filename}}{Path where the object is stored absolute or relative to the storage path.}
#' \item{\strong{single_object}}{Logical. \code{TRUE} if the archive contain only this one object. Otherwise
#' archive contains named list of objects.}
#' }
#' @export
list_runtime_objects<-function(storagepath) {
  path<-get_runtime_index_path(storagepath)
  if(file.exists(path)) {
    idx<-readRDS(path)
    return(idx)
  } else {
    return(data.table(objectnames=character(0), digest=character(0),
                      size=numeric(0), archive_filename=character(0),
                      single_object=logical(0)))
  }
}



#' Adds or removes objects in the storage.
#'
#' @param storagepath Path with the storage.
#' @param obj.environment Environment or named list with the objects
#' @param addobjectnames Character vector with the names of the objects to add. Defaults to all objects in the
#' \code{obj.environment}.
#' @param removeobjectnames Character vector with the names of the objects to remove. Cannot contain objects
#' listed in \code{addobjectnames}.
#' @param archive_filename
#' @return Returns `data.frame` with the following columns:
#' \describe{
#' \item{\strong{objectname}}{Name of the stored object. This is a primary key.}
#' \item{\strong{digest}}{String with the digest of the object.}
#' \item{\strong{size}}{Numeric value with the size of the stored object.}
#' \item{\strong{archive_filename}}{Path where the object is stored absolute or relative to the storage path.}
#' \item{\strong{single_object}}{Logical. \code{TRUE} if the archive contain only this one object. Otherwise
#' archive contains named list of objects.}
#' }
#' @export

# Na wejściu otrzymujemy listę archives list, która dla każdego archiwum zawiera listę z elementami
# objectnames - lista objektów, archive_filename - ścieżka do pliku archiwum, compress, flag_use_tmp_storage

modify_runtime_objects<-function(storagepath, obj.environment, objects_to_add=NULL, objects_to_remove=character(0),
                                 flag_forced_save_filenames=FALSE, flag_use_tmp_storage=FALSE,
                                 forced_archive_paths=NA, compress='gzip', large_archive_prefix=NULL,
                                 flag_no_nested_folder=FALSE,
                                 locktimeout=NULL,
                                 wait_for='save',parallel_cpus=NULL)
{
  archives_list<-infer_save_locations(storagepath = storagepath, objectnames = objects_to_add,
                                      obj.environment = obj.environment,
                                      flag_forced_save_filenames = flag_forced_save_filenames,
                                      flag_use_tmp_storage = flag_use_tmp_storage,
                                      forced_archive_paths = forced_archive_paths,
                                      compress = compress, large_archive_prefix = large_archive_prefix,
                                      flag_no_nested_folder = flag_no_nested_folder)

  add_runtime_objects_internal(storagepath = storagepath, obj.environment = obj.environment,
                               archives_list = archives_list, parallel_cpus = parallel_cpus,
                               removeobjectnames = objects_to_remove,
                               locktimeout = locktimeout, wait_for = wait_for)
  return(storagepath)
}

#archives_list - output from infer_save_locations()
add_runtime_objects_internal<-function(storagepath, obj.environment, archives_list,
                                       removeobjectnames=character(0),
                                       locktimeout=NULL,
                                       wait_for='save',parallel_cpus=NULL)
{
  archives_db<-lists_to_df(archives_list, list_columns='objectnames')
  archives_db_flat<-data.table(tidyr::unnest(archives_db),
                               digest=NA_character_, size=NA_real_)
  archives_db_flat<-purrrlyr::by_row(archives_db_flat, ~length(.$objectnames[[1]])>1, .collate = 'cols', .to='single_object')


  objectnames<-archives_db_flat$objectnames

  for(i in seq_along(objectnames)) {
    objname<-objectnames[[i]]
    set(archives_db_flat, i, 'digest', calculate.object.digest(objname, obj.environment,
                                                               flag_use_attrib = FALSE, flag_add_attrib = TRUE))
    set(archives_db_flat, i, 'size', object.size(obj.environment[[objname]]))
  }

  flag_do_sequentially=FALSE
  if(!is.null(parallel_cpus)) {
    if(parallel_cpus==0) {
      flag_do_sequentially=TRUE
    }
  }


  if(lock.exists(storagepath, locktimeout)) {
    cat("Waiting to get the lock for ", storagepath, "...\n")
  }
  create.lock.file(storagepath, locktimeout)
  tryCatch({

    oldidx<-list_runtime_objects(storagepath = storagepath)

    #We need to create two lists of objects for each archive:
    #List of objects to remove
    #List of objects to add

    changed_objects_db<-dplyr::inner_join(oldidx, archives_db_flat, by=c(objectnames='objectnames'), suffix=c("_old", "_new"))

    #We remove objects that are contained in the `removeobjectnames` argument,
    #plus objects that changed the archive
    removeobjectnames_db<-data.table(objectnames=removeobjectnames)
    remove_objects_db<-rbind(
      dplyr::select(
        dplyr::filter(changed_objects_db, archive_filename_new!=archive_filename_old),
        objectnames, archive_filename=archive_filename_old),
      dplyr::select(
        dplyr::inner_join(oldidx, removeobjectnames_db, by=c(objectnames='objectnames')),
        objectnames, archive_filename)
    )
    #We add objects that are new
    #plus those who are common and have their digest changed
    new_objects_db<-dplyr::select(
      dplyr::anti_join(archives_db_flat, oldidx, by=c(objectnames='objectnames')),
      objectnames, archive_filename)
    different_objects_db<-dplyr::select(
      dplyr::filter(changed_objects_db, digest_old!=digest_new),
      objectnames, archive_filename=archive_filename_new)
    # if(nrow(different_objects_db)>0) {
    #   fn_get_digest<-function(objectname) {
    #     calculate.object.digest(objectname = objectname, target.environment = obj.environment,
    #                             flag_use_attrib=TRUE, flag_add_attrib=FALSE)
    #   }
    #   if(flag_do_sequentially) {
    #     digests<-lapply(different_objects_db$objectname, fn_get_digest)
    #   } else {
    #     digests<-parallel::mclapply(different_objects_db$objectname, fn_get_digest)
    #   }
    #   different_objects_db$digest_new<-as.character(digests)
    #
    #   different_objects_db<-dplyr::select(
    #     dplyr::filter(different_objects_db, digest_new==digest_old),
    #     objectnames, archive_filename)
    # }

    new_objects_db<-rbind(new_objects_db, different_objects_db)

    #Now we need to group new_objects_db and db_to_remove by archive_filename, and apply it

    new_objects_db_nested<-
      dplyr::select(
        dplyr::mutate(
          tidyr::nest(
            dplyr::group_by(new_objects_db,archive_filename),
            objectnames),
          objectnames = purrr::map(data, ~.$objectnames)),
        -data
      )

    remove_objects_db_nested<-
      dplyr::select(
        dplyr::mutate(
          tidyr::nest(
            dplyr::group_by(remove_objects_db,archive_filename),
            objectnames),
          objectnames = purrr::map(data, ~.$objectnames)),
        -data
      )

    change_objects_db_nested<-dplyr::full_join(new_objects_db_nested, remove_objects_db_nested,
                                               by=c(archive_filename='archive_filename'),
                                               suffix=c('_new', '_remove'))
    change_objects_db_nested<-dplyr::select(
      dplyr::left_join(change_objects_db_nested, archives_db,
                       by=c(archive_filename='archive_filename')),
      -objectnames)
    #browser()

    if(flag_do_sequentially) {
      ans<-mapply(modify_runtime_archive,
                  addobjectnames=change_objects_db_nested$objectnames_new,
                  removeobjectnames=change_objects_db_nested$objectnames_remove,
                  archive_filename=change_objects_db_nested$archive_filename,
                  compress=change_objects_db_nested$compress,
                  flag_use_tmp_storage=change_objects_db_nested$flag_use_tmp_storage,
                  MoreArgs=list(storagepath=storagepath, obj.environment=obj.environment,
                                wait_for=wait_for,
                                parallel_cpus=parallel_cpus),
                  SIMPLIFY=FALSE)
    } else {
      ans<-parallel::mcmapply(modify_runtime_archive,
                              addobjectnames=change_objects_db_nested$objectnames_new,
                              removeobjectnames=change_objects_db_nested$objectnames_remove,
                              archive_filename=change_objects_db_nested$archive_filename,
                              compress=change_objects_db_nested$compress,
                              flag_use_tmp_storage=change_objects_db_nested$flag_use_tmp_storage,
                              MoreArgs=list(storagepath=storagepath, obj.environment=obj.environment,
                                            wait_for=wait_for,
                                            parallel_cpus=parallel_cpus),
                              SIMPLIFY=FALSE)
    }
    jobs<-list()
    oldidx<-oldidx[!oldidx$archive_filename %in% change_objects_db_nested$archive_filename,]
    for(i in seq_along(ans)) {
      jobs[[i]]<-ans[[i]]$job
      oldidx<-rbind(oldidx, ans[[i]]$dbchunk)
    }
    if(length(jobs)>0) {

      pb <- txtProgressBar(min = 0, max = length(jobs), style = 3)
      cat("Waiting for saves to finish..")
      parallel::mccollect(jobs, wait=TRUE, intermediate = function(res) {setTxtProgressBar(pb, length(res))})
      close(pb)
    }
    update_runtime_objects_index(storagepath = storagepath, newidx=oldidx)
  }, finally=release.lock.file(storagepath))
}

