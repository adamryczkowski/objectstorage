
# nocov start
.onLoad	<-	function(libname,	pkgname)	{
  op	<-	options()
  op.objectstorage	<-	list(
    objectstorage.lock_extension	=	'.lock',
    objectstorage.index_extension = '.rdx',
    objectstorage.default_archive.extension = '.rda', #Used only for the main archive for small objects
    objectstorage.reserved_attr_for_hash ='..hash',
    objectstorage.prefix_for_automatic_dedicated_archive_names='_',
    objectstorage.default_archive_name = '_default_archive.rda', #Name of the folder to save objects if more than one
    objectstorage.default_lock_time = 3600, #1 hour
    objectstorage.tune_threshold_objsize_for_dedicated_archive = 20000 #Results from `studium_save` multiplied by 4.
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
    idx<-empty_objectstorage()
    path<-get_runtime_index_path(storagepath)
    saveRDS(object = idx, file = path)
  } else {
    idx<-list_runtime_objects(storagepath)
  }
  return(idx)
}

empty_objectstorage<-function() {
  idx<-data.table(objectnames=character(0), digest=character(0),
                  size=numeric(0), archive_filename=character(0),
                  single_object=logical(0), compress=character(0),
                  flag_use_tmp_storage=logical(0))
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
    return(empty_objectstorage())
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
#' @param archive_filename Optional character vector with custom paths to the archives. Can be a single character object,
#'        vector with the size of \code{objects_to_add} or named vector with keys from \code{objects_to_add}.
#' @param flag_forced_save_filenames Controls, whether force a particular object in its own dedicated archive.
#' Value can be either single boolean, or vector of booleans with the same size as
#' \code{addobjectnames}, or named boolean vector with keys values of \code{addobjectnames}. In the latter case,
#' non-mentioned objects will be assumed value \code{FALSE} (i.e. not forced filename).
#' @param compress Controls the compression of the archive. It is important to realize, that if the archive
#' had already contained some objects prior to modifying it and the modification would not remove the objects,
#' those objects will be re-compressed with the \code{compress} compression, since the archive will effectively
#' be re-added. Supports 3 calues: \code{none}, \code{gzip} and \code{xz}.
#' Value can be either single character, or vector of characters with the same size as
#' \code{addobjectnames}, or named character vector with keys values of \code{addobjectnames}. In the latter case,
#' non-mentioned objects will be assumed value \code{gzip}.
#' @param large_archive_prefix If set, all new archives for large objects will be saved with this prefix, otherwise in the
#' \code{dirname(storagepath)}.
#' \code{storagepath}. It is up to the user to make sure this directory is empty and no file name conflicts will
#' arise.
#' @return Returns `data.frame` with the following columns:
#' \describe{
#' \item{\strong{objectname}}{Name of the stored object. This is a primary key.}
#' \item{\strong{digest}}{String with the digest of the object.}
#' \item{\strong{size}}{Numeric value with the size of the stored object.}
#' \item{\strong{archive_filename}}{Path where the object is stored absolute or relative to the storage path.}
#' \item{\strong{single_object}}{Logical. \code{TRUE} if the archive contain only this one object. Otherwise
#' archive contains named list of objects.}
#' \item{\strong{compress}}{Type of compression used to store this individual object}
#' }
#' @export

# Na wejściu otrzymujemy listę archives list, która dla każdego archiwum zawiera listę z elementami
# objectnames - lista objektów, archive_filename - ścieżka do pliku archiwum, compress, flag_use_tmp_storage
modify_objects<-function(storagepath, obj.environment, objects_to_add=NULL, objects_to_remove=character(0),
                         flag_forced_save_filenames=FALSE, flag_use_tmp_storage=FALSE,
                         forced_archive_paths=NA, compress='gzip', large_archive_prefix=NULL,
                         locktimeout=NULL,
                         wait_for='save',parallel_cpus=NULL)
{
  if(is.null(obj.environment)) {
    obj.environment<-new.env(parent=emptyenv())
  }
  archives_list<-infer_save_locations(storagepath = storagepath, objectnames = objects_to_add,
                                      obj.environment = obj.environment,
                                      flag_forced_save_filenames = flag_forced_save_filenames,
                                      flag_use_tmp_storage = flag_use_tmp_storage,
                                      forced_archive_paths = forced_archive_paths,
                                      compress = compress, large_archive_prefix = large_archive_prefix)
  if(!is.null(archives_list)) {
#    browser()
    add_runtime_objects_internal(storagepath = storagepath, obj.environment = obj.environment,
                                 archives_list = archives_list, parallel_cpus = parallel_cpus,
                                 removeobjectnames = objects_to_remove,
                                 locktimeout = locktimeout, wait_for = wait_for)
  }
  return(storagepath)
}


#' Replaces objects in the storage.
#'
#' @param storagepath Path with the storage.
#' @param obj.environment Environment or named list with the objects
#' @param objectnames Optional character vector with the names of the objects to add. Defaults to all objects in the
#' \code{obj.environment}.
#' @param archive_filename Optional character vector with custom paths to the archives. Can be a single character object,
#'        vector with the size of \code{objectnames} or named vector with keys from \code{objectnames}.
#' @param flag_forced_save_filenames Optional boolean vector. Controls, whether force a particular object in its own dedicated archive.
#' Value can be either single boolean, or vector of booleans with the same size as
#' \code{objectnames}, or named boolean vector with keys values of \code{objectnames}. In the latter case,
#' non-mentioned objects will be assumed value \code{FALSE} (i.e. not forced filename).
#' @param compress Controls the compression of the archive. It is important to realize, that if the archive
#' had already contained some objects prior to modifying it and the modification would not remove the objects,
#' those objects will be re-compressed with the \code{compress} compression, since the archive will effectively
#' be re-added. Supports 3 calues: \code{none}, \code{gzip} and \code{xz}.
#' Value can be either single character, or vector of characters with the same size as
#' \code{objectnames}, or named character vector with keys values of \code{objectnames}. In the latter case,
#' non-mentioned objects will be assumed value \code{gzip}.
#' @param large_archive_prefix If set, all new archives for large objects will be saved with this prefix, otherwise in the
#' \code{dirname(storagepath)}.
#' \code{storagepath}. It is up to the user to make sure this directory is empty and no file name conflicts will
#' arise.
#' @return Returns `data.frame` with the following columns:
#' \describe{
#' \item{\strong{objectname}}{Name of the stored object. This is a primary key.}
#' \item{\strong{digest}}{String with the digest of the object.}
#' \item{\strong{size}}{Numeric value with the size of the stored object.}
#' \item{\strong{archive_filename}}{Path where the object is stored absolute or relative to the storage path.}
#' \item{\strong{single_object}}{Logical. \code{TRUE} if the archive contain only this one object. Otherwise
#' archive contains named list of objects.}
#' \item{\strong{compress}}{Type of compression used to store this individual object}
#' }
#' @export
save_objects<-function(storagepath, obj.environment, objectnames=NULL,
                         flag_forced_save_filenames=FALSE, flag_use_tmp_storage=FALSE,
                         forced_archive_paths=NA, compress='gzip', large_archive_prefix=NULL,
                         locktimeout=NULL,
                         wait_for='save',parallel_cpus=NULL)
{
  #browser()
  if(is.null(objectnames)) {
    objectnames<-names(obj.environment)
  }
  all_objects<-list_runtime_objects(storagepath)
  objects_to_add <- intersect(objectnames, names(obj.environment))
  objects_to_remove <- setdiff(all_objects$objectnames, objects_to_add)

  archives_list<-infer_save_locations(storagepath = storagepath, objectnames = objects_to_add,
                                      obj.environment = obj.environment,
                                      flag_forced_save_filenames = flag_forced_save_filenames,
                                      flag_use_tmp_storage = flag_use_tmp_storage,
                                      forced_archive_paths = forced_archive_paths,
                                      compress = compress, large_archive_prefix = large_archive_prefix)
  if(!is.null(archives_list) || length(objects_to_remove)>0) {
    add_runtime_objects_internal(storagepath = storagepath, obj.environment = obj.environment,
                                 archives_list = archives_list, parallel_cpus = parallel_cpus,
                                 removeobjectnames = objects_to_remove,
                                 locktimeout = locktimeout, wait_for = wait_for)
  }
  return(storagepath)
}

#' Removes everything from disk
#'
#' @param storagepath Path to the storage
#' @export

remove_all<-function(storagepath) {
  all_objects<-list_runtime_objects(storagepath = storagepath)$objectname
  modify_objects(storagepath, objects_to_remove = all_objects, obj.environment = NULL)
  path<-get_runtime_index_path(storagepath)
  unlink(path)
}


#' Sets new contents of the objectstorage.
#'
#' The input objects will be compared with the stored objects, and replaced only when needed
#'
#' @param storagepath Path with the storage.
#' @param obj.environment Environment or named list with the objects
#' @param objectnames Character vector with the names of the objects to add. Defaults to all objects in the
#' \code{obj.environment}.
#' @param flag_forced_save_filenames Boolean vector with the length equal to objectnames, or
#'        named vector with objectnames as keys, or single value specifying whether to put a specific object
#'        in its own dedicated archive
#' @param forced_archive_paths Character vector with the length equal to objectnames, or
#'        named vector with objectnames as keys, or single value specifying custom path of archive
#'        where a specific object(s) will saved.
#' @param flag_use_tmp_storage Boolean vector with the length equal to objectnames, or
#'        named vector with objectnames as keys, or single value specifying whether if the temporary
#'        save file should be created in the fast /tmp directory first, and only then compressed into
#'        the target place.
#' @param compress Character vector with the length equal to objectnames, or
#'        named vector with objectnames as keys, or single value specifying compression algorithm for
#'        each object. Compression will be applied archive-wise.
#' @param large_archive_prefix If set, all new archives for large objects will be saved with this prefix, otherwise in the
#' \code{dirname(storagepath)}.
#' \code{storagepath}. It is up to the user to make sure this directory is empty and no file name conflicts will
#' arise.
#' @return Returns `data.frame` with the following columns:
#' \describe{
#' \item{\strong{objectname}}{Name of the stored object. This is a primary key.}
#' \item{\strong{digest}}{String with the digest of the object.}
#' \item{\strong{size}}{Numeric value with the size of the stored object.}
#' \item{\strong{archive_filename}}{Path where the object is stored absolute or relative to the storage path.}
#' \item{\strong{single_object}}{Logical. \code{TRUE} if the archive contain only this one object. Otherwise
#' archive contains named list of objects.}
#' \item{\strong{compress}}{Type of compression used to store this individual object}
#' }
#' @export

# Na wejściu otrzymujemy listę archives list, która dla każdego archiwum zawiera listę z elementami
# objectnames - lista objektów, archive_filename - ścieżka do pliku archiwum, compress, flag_use_tmp_storage
set_runtime_objects<-function(storagepath, obj.environment, objectnames=NULL,
                                 flag_forced_save_filenames=FALSE, flag_use_tmp_storage=FALSE,
                                 forced_archive_paths=NA, compress='gzip', large_archive_prefix=NULL,
                                 locktimeout=NULL,
                                 wait_for='save',parallel_cpus=NULL)
{
  archives_list<-infer_save_locations(storagepath = storagepath, objectnames = objectnames,
                                      obj.environment = obj.environment,
                                      flag_forced_save_filenames = flag_forced_save_filenames,
                                      flag_use_tmp_storage = flag_use_tmp_storage,
                                      forced_archive_paths = forced_archive_paths,
                                      compress = compress, large_archive_prefix = large_archive_prefix)

  oldidx<-list_runtime_objects(storagepath = storagepath)
  objects_to_remove<-setdiff(oldidx$objectrecords, objectnames)

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
  archives_db<-lists2df::lists_to_df(archives_list, list_columns='objectnames')
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
    movedobjects<-dplyr::select(
      dplyr::filter(changed_objects_db, archive_filename_new!=archive_filename_old),
      objectnames, archive_filename=archive_filename_old)
    remove_objects_db<-rbind(
      movedobjects,
      dplyr::select(
        dplyr::inner_join(oldidx, removeobjectnames_db, by=c(objectnames='objectnames')),
        objectnames, archive_filename)
    )
    #We add objects that are new
    #plus those who are common and have their digest changed
    #plus those, who are moved
    new_objects_db<-dplyr::select(
      dplyr::anti_join(archives_db_flat, oldidx, by=c(objectnames='objectnames')),
      objectnames, archive_filename)
    different_objects_db<-dplyr::select(
      dplyr::filter(changed_objects_db, digest_old!=digest_new | archive_filename_new!=archive_filename_old),
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

#' Returns mtime of last modification of the \code{objectstorage}
#'
#' @param storagepath Path with the storage.
#' @return Returns data stamp of typ \code{c("POSIXct", "POSIXt")} of the modification time.
#' @export
get_mtime<-function(storagepath) {
  path<-get_runtime_index_path(storagepath=storagepath)
  if(file.exists(path)) {
    return(file.mtime(path))
  } else {
    return(NA)
  }
}

#' Returns md5 hash of all the objects in the \code{objectstorage} stored in disk.
#'
#' This function is almost instantenous even for large objects, because it doesn't
#' read the actual objects, it reads their hashes from the description file.
#'
#' Actually this ability to quickly get the cryptographic hash of all objects contained within
#' the archive is a motivation behind
#'
#' @param storagepath Path with the storage.
#' @return Returns data stamp of typ \code{c("POSIXct", "POSIXt")} of the modification time.
#' @export
get_full_digest<-function(storagepath) {
  path<-get_runtime_index_path(storagepath=storagepath)
  if(file.exists(path)) {
    idx<-list_runtime_objects(storagepath = storagepath)
    objectnames<-idx$objectnames
    digests<-idx$digest
    ord<-order(objectnames)
    digests<-digests[ord]
    objectnames<-objectnames[ord]
    todigest<-setNames(digests, nm = objectnames)
    return(digest::digest(todigest))
  } else {
    return(NA)
  }
}

#' Returns md5 hash of all the objects in the \code{objectstorage} stored in disk.
#'
#' This function is almost instantenous even for large objects, because it doesn't
#' read the actual objects, it reads their hashes from the description file.
#'
#' Actually this ability to quickly get the cryptographic hash of all objects contained within
#' the archive is a motivation behind
#'
#' @param storagepath Path with the storage.
#' @return Returns data stamp of typ \code{c("POSIXct", "POSIXt")} of the modification time.
#' @export
get_object_digest<-function(storagepath, objectnames) {
  path<-get_runtime_index_path(storagepath=storagepath)
  if(file.exists(path)) {
    idx<-list_runtime_objects(storagepath = storagepath)
    out<-dplyr::left_join(tibble::tibble(objectnames=objectnames), idx, by=c(objectnames='objectnames'))
    return(out$digest)
  } else {
    stop(paste("There is no object storage in ", storagepath))
  }
}


#'Main function to load the objects into the given environment
#'
#' @param storagepath Path to the storage metadata
#' @param objectnames Objectnames to extract
#' @param aliasnames Optional vector of the same length as \code{objectnames} with new names for the extracted objects.
#' @param target_environment Target environment where to put the objects
#' @param flag_double_check_digest Calculate the digest of the extracted objects and check it against the metadata
#' @return Logical vector, one for each loaded object. \code{TRUE} means that
#'         load was successfull, \code{FALSE} otherwise.
#' @export
load_objects<-function(storagepath, objectnames=NULL, target_environment, flag_double_check_digest=FALSE, aliasnames=NULL) {
  tmppath<-get_runtime_index_path(storagepath)
  assertValidPath(tmppath)

  df<-list_runtime_objects(storagepath = storagepath)
  if(is.null(objectnames)) {
    objectnames<-df$objectname
  } else {
    if(length(setdiff(objectnames, df$objectname))>0) {
      stop(paste0("Objects ", paste0(setdiff(objectnames, df$objectname), collapse = ','),
                  " are missing from the objectstorage. Are you sure you have put them there?"))
    }
  }


  checkmate::assertCharacter(objectnames)
  checkmate::assertFALSE(any(duplicated(objectnames)))
  checkmate::assertEnvironment(target_environment)
  checkmate::assertFlag(flag_double_check_digest)
  if(!is.null(aliasnames)) {
    checkmate::assertCharacter(aliasnames)
    checkmate::assertFALSE(any(duplicated(aliasnames)))
    checkmate::assertTRUE(length(aliasnames)==length(objectnames))
  } else {
    aliasnames<-objectnames
  }

  idx<-list_runtime_objects(storagepath)
  idx_f<-dplyr::filter(idx, objectnames %in% objectnames)
  if(length(setdiff(objectnames, idx$objectnames))>0) {
    stop("There is no ", paste0(setdiff(objectnames, idx$objectnames), collapse=", "), " objects in the storage!")
  }
  idx_gr<-tidyr::nest(dplyr::group_by(idx, archive_filename))
  for(i in seq(1, nrow(idx_gr))) {
    archivepath<-idx_gr$archive_filename[[i]]
    data<-idx_gr$data[[i]]
    single_object<-data$single_object[[1]]
    if(single_object) {
      if(nrow(data)>1) {
        browser() #Something wrong with the records. There should be only single object
      }
      archivepath<-pathcat::path.cat(dirname(storagepath), archivepath)
      newname<-aliasnames[which(objectnames==data$objectnames[[1]])]
      assign(x = newname, value = readRDS(archivepath), envir = target_environment)
      if(flag_double_check_digest) {
        d1<-calculate.object.digest(newname, target.environment = target_environment,
                                    flag_use_attrib = FALSE, flag_add_attrib = FALSE)
        d2<-data$digest[[1]]
        if(d1!=d2) {
          browser()
          stop(paste0("Object ", newname, " stored in ", archivepath, " has digest ", d1, ", ",
                      "which doesn't match stored digest of ", d2))
        }
      }

    } else {
      if(nrow(data)==0) {
        browser() #internal error
      }
      alldb<-dplyr::filter(idx, archive_filename==archivepath)
      env<-new.env()
      archivepath<-pathcat::path.cat(dirname(storagepath), archivepath)
      assign(x = 'obj', value = readRDS(archivepath), envir = env)
      data<-data[data$objectnames %in% objectnames,]

      for(i in seq(1,nrow(data))) {
        objname<-data$objectnames[[i]]
        newname<-aliasnames[which(objectnames==data$objectnames[[i]])]
        if(!objname %in% names(env$obj)) {
          stop(paste0("Cannot find object ", objname, " among objects actually saved in ",
                      archivepath, " although advertised this object should be there."))
        }
        assign(x=newname, value=env$obj[[objname]], envir=target_environment)
        if(flag_double_check_digest) {
          d1<-calculate.object.digest(newname, target.environment = env$obj,
                                      flag_use_attrib = FALSE, flag_add_attrib = FALSE)
          d2<-data$digest[[i]]
          if(d1!=d2) {
            stop(paste0("Object ", objname, " stored in ", archivepath, " has digest ", d1, ", ",
                        "which doesn't match stored digest of ", d2))
          }
        }
      }
    }
  }
  return(rep(TRUE, length(objectnames)))
}
