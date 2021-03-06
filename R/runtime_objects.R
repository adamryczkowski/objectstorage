#File contain all the procedures involved in dealing with runtime_objects
#
#Index file contains a data frame with the following columns:
#objectname - name of the object contained
#digest - digest of it
#size - size in bytes in memory
#archive_filename - path to the archive
#single_object - flag if the object is a single object in the archive (and not a part of the nested list)
#


update_runtime_objects_index<-function(storagepath, newidx) {
  path<-get_runtime_index_path(storagepath)
  if(file.exists(path)) {
    oldidx<-readRDS(path)
    if(identical(newidx, oldidx)) {
      return()
    }
    if(nrow(oldidx)>0) {
#      browser()
    }
    unlink(path)
  }
  objects<-newidx$objectnames
  if(sum(duplicated(objects))==0) {
    saveRDS(newidx, path)
  } else {
    browser()
    stop("Duplication in the objectnames in the index that is about to be written!")
  }
}



get_runtime_index_path<-function(storagepath) {
  ext<-getOption('objectstorage.index_extension')
  ext2<-stringr::str_replace(ext, pattern=stringr::fixed('.'), replacement = '\\.')
  if(!stringr::str_detect(storagepath, stringr::regex(paste0(ext2, '$')))) {
    path<-paste0(storagepath, ext)
  } else {
    path<-storagepath
  }
  path<-pathcat::path.cat(getwd(), path)
  assertValidPath(path)
  return(path)
}

remove_runtime_object<-function(storagepath, objname) {
  idx<-list_runtime_objects(path)
  objdigest<-calculate.object.digest(objname, obj.environment)
  idx[[objname]]<-list(name=objname,
                       size=object.size(obj.environment[[objname]]),
                       digest=objdigest)
  update_runtime_objects_index(storagepath, idx)
}


#' Generates plan of where and how save objects, which is used by other low-level functions
#'
#' @param storagepath Path with the storage.
#' @param objectnames Character vector with the names of the objects to add. Defaults to all objects in the
#' \code{obj.environment}.
#' @param obj.environment Environment or named list with the objects. It is needed only for getting
#' the object sizes in case user selects the default algorithm.
#' @param flag_forced_save_filenames Controls, whether force a particular object in its own dedicated archive.
#' Value can be either single boolean, or vector of booleans with the same size as
#' \code{objectnames}, or named boolean vector with keys values of \code{objectnames}. In the latter case,
#' non-mentioned objects will be assumed value \code{FALSE} (i.e. not forced filename).
#' @param forced_archive_paths Overrides a specific path for the object. More than one object can be given
#' the same path - in that case the archive will be of the "multiple objects" type. This override is a stronger
#' version of parameter \code{flag_forced_save_filenames} - the difference is that it allows for manual naming
#' and locating the archives.
#' Value can be either single character, or vector of characters with the same size as
#' \code{objectnames}, or named character vector with keys values of \code{objectnames}. In the latter case,
#' non-mentioned objects will be assumed value \code{NA} (i.e. not having forced filename).
#' @param flag_use_tmp_storage Relevant only for \code{xz} compression with the external tool \code{pxz}.
#' Normally the object will be quickly saved without compression, then compressed in the background
#' and at the end the filenames will be swapped. Setting this flag will force saving in the quick \code{\\tmp}
#' directory, instead of the target path. It is usefull if the target path is very slow (perhaps a distant
#' network share).
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
#' @param default_save_directory_suffix Default location for each saved archive, absolute or relative to the
#' \code{storagepath}. Defaults to \code{''}, i.e. the objects will be saved in the same place as the \code{storagepath}.
#' @param large_archive_prefix If set, all new archives for large objects will be saved with this prefix, otherwise in the
#' \code{dirname(storagepath)}.
#' \code{storagepath}. It is up to the user to make sure this directory is empty and no file name conflicts will
#' arise.
#' @return Returns list with one element for each archive to touch. Each element will be a list with the
#' following properties
#' \describe{
#' \item{\strong{objectnames}}{Character vector with one or more object names that are going to be stored in this archive.}
#' \item{\strong{archive_filename}}{Character vector with the path to the archive, stored as relative path to the \code{storagepath}}
#' \item{\strong{compress}}{Compression method for all files}
#' \item{\strong{flag_use_tmp_storage}}{Whether or not use the temporary storage}
#' }
#' @export
infer_save_locations<-function(storagepath, objectnames=NULL, obj.environment,
                               flag_forced_save_filenames=FALSE, flag_use_tmp_storage=FALSE,
                               forced_archive_paths=NA, compress='gzip', large_archive_prefix=NULL)
{
  if(is.null(objectnames)) {
    objectnames<-names(obj.environment)
  }

  if(length(objectnames)==0) {
    return(NULL)
  }
  ext<-getOption('objectstorage.index_extension')
  ext2<-stringr::str_replace(ext, pattern=stringr::fixed('.'), replacement = '\\.')
  if(stringr::str_detect(storagepath, stringr::regex(paste0(ext2, '$')))) {
    storagepath<-stringr::str_replace(storagepath, pattern = stringr::regex(paste0(ext2, '$')), replacement = '')
  }

  flag_forced_save_filenames<-parse_argument(arg=flag_forced_save_filenames,
                                             objectnames=objectnames, default_value=FALSE)
  forced_archive_paths<-parse_argument(arg=forced_archive_paths,
                                       objectnames=objectnames, default_value=NA_character_)
  compress<-parse_argument(arg=compress,
                           objectnames=objectnames, default_value='gzip')
  flag_use_tmp_storage<-parse_argument(arg=flag_use_tmp_storage,
                                       objectnames=objectnames, default_value=FALSE)



  flag_forced_save_filenames[names(forced_archive_paths)[!is.na(forced_archive_paths)] ]<-TRUE
  default_objects<-objectnames
  out<-list() # list of all object names
  if(sum(flag_forced_save_filenames)>0) {
    all_containers<-unique(forced_archive_paths)
    for(i in seq(1, length(all_containers))){
      cntname<-pathcat::path.cat(dirname(storagepath), all_containers[[i]])
      cntname<-pathcat::make.path.relative(dirname(storagepath), cntname)
      poss<-which(forced_archive_paths==cntname)
      cnt_objnames<-objectnames[poss] #object names that are stored in the forced archive name
      out[[cntname]]<-list(objectnames=cnt_objnames, archive_filename=cntname,
                           compress=compress[poss],
                           flag_use_tmp_storage=flag_use_tmp_storage[poss])
      default_objects<-setdiff(default_objects, cnt_objnames)
    }
  }
  #Now we can be sure, that there are no objects with forced archive paths in default_objects; those items has been handled already.

  if(length(default_objects)>0) {
    flag_forced_save_filenames<-flag_forced_save_filenames[default_objects]
    compress<-compress[default_objects]
    flag_use_tmp_storage<-flag_use_tmp_storage[default_objects]
    objectnames<-default_objects
    objectsizes<-purrr::map_dbl(objectnames, ~object.size(obj.environment[[.]]))
    flag_forced_save_filenames[objectsizes > getOption('objectstorage.tune_threshold_objsize_for_dedicated_archive')]<-TRUE

    #First we take care of all the small objects that are going to be saved in the default location
    number_of_files<-sum(flag_forced_save_filenames)
    generic_file_name<-pathcat::path.cat(dirname(storagepath), paste0(basename(storagepath), getOption('objectstorage.default_archive.extension')))

    generic_file_name<-pathcat::make.path.relative(dirname(storagepath), generic_file_name)

    if(sum(!flag_forced_save_filenames)>0) {
      item=list(objectnames=c(objectnames[!flag_forced_save_filenames], out[[generic_file_name]]$objectnames),
                archive_filename=generic_file_name,
                compress=as.character(c(compress[!flag_forced_save_filenames], out[[generic_file_name]]$compress)),
                flag_use_tmp_storage=as.logical(c(flag_use_tmp_storage[!flag_forced_save_filenames], out[[generic_file_name]]$flag_use_tmp_storage)))
      out[[generic_file_name]]<-item
    }

    if(sum(flag_forced_save_filenames)>0) {
      separate_objects<-objectnames[flag_forced_save_filenames]
      separate_paths<-pathcat::path.cat(dirname(storagepath),
                                        paste0(getOption('objectstorage.prefix_for_automatic_dedicated_archive_names'),
                                               separate_objects, ".rds"))
      separate_paths<-pathcat::make.path.relative(dirname(storagepath), separate_paths)
      separate_compress<-compress[flag_forced_save_filenames]
      separate_flag_use_tmp_storage<-flag_use_tmp_storage[flag_forced_save_filenames]

      for(i in seq(1, length(separate_objects))) {
        obj_name<-separate_objects[[i]]
        path<-separate_paths[[i]]
        out[[path]]<-c(
          out[[path]],
          list(objectnames=obj_name, archive_filename=path,
               compress=separate_compress[[i]],
               flag_use_tmp_storage=separate_flag_use_tmp_storage[[i]]))
      }
    }
  }
  if(length(out)>1) {
    df<-as.data.frame(table(names(out)))
  } else {
    df<-data.frame(Var1=names(out), Freq=1)
  }
  dups<-dplyr::filter(df, Freq>1)
  if(nrow(dups)>0){
    nondups<-dplyr::filter(df, Freq==1)
    if(nrow(nondups)>0) {
      newout<-out[nondups$Var1 ]
    } else {
      newout<-list()
    }
    for(i in seq(1, nrow(dups))) {
      cname<-dups$Var1[[i]]
      poss<-which(names(out)==cname)
      dic<-out[poss]

      dicdf<-list_to_df(dic, list_columns=setdiff(names(dic), 'archive_filename'))
      outnew[[cname]]<-list(objectnames=do.call(c, dicdf$objectnames),
                            archive_filename=cname,
                            compress=do.call(c,dicdf$compress),
                            flag_use_tmp_storage=do.call(c,dicdf$flag_use_tmp_storage)
      )
    }
    out<-newout
  }

  for(i in seq_along(out)) {
    item<-out[[i]]
    if(length(unique(item$compress))!=1) {
      stop(paste0("Non-unique elements in the argument compress for runtime.object saved in ",
                  item$archive_filename))
    }
    if(length(unique(item$flag_use_tmp_storage))!=1) {
      stop(paste0("Non-unique elements in the argument flag_use_tmp_storage for runtime.object saved in ",
                  item$archive_filename))
    }
    out[[i]]<-list(archive_filename=item$archive_filename,
                   objectnames=item$objectnames,
                   compress=item$compress[[1]],
                   flag_use_tmp_storage=item$flag_use_tmp_storage[[1]])
  }

  return(out)
}

modify_runtime_archive<-function(storagepath, obj.environment, addobjectnames=NULL,
                                 removeobjectnames=character(0),
                                 archive_filename, compress='gzip', wait_for='save',
                                 flag_use_tmp_storage=FALSE, parallel_cpus=NULL) {
  if(is.null(objectnames)){
    objectnames<-names(obj.environment)
  }

  if(length(objectnames)==0 && length(removeobjectnames)==0) {
    return() #nothing to do
  }
  archivepath<-pathcat::path.cat(dirname(tasktpath), archive_filename)

  if(length(intersect(removeobjectnames, addobjectnames))>0) {
    browser()
    #Makes no sense in adding and deleting the same object in one step
  }

  idx<-dplyr::filter(list_runtime_objects(storagepath), archive_filename==archive_filename)
  objs_to_leave<-setdiff(idx$objectnames, c(addobjectnames, removeobjectnames))
  objs_to_add<-c(addobjectnames,objs_to_leave)
  if(length(objs_to_leave)>0) {
    obj.environment<-new.env(parent = obj.environment)
    oldobjs<-readRDS(archivepath)
    for(i in seq_along(objs_to_leave)) {
      obj_to_leave<-objs_to_leave[[i]]
      rowpos<-which(idx$objectnames==obj_to_leave)
      if(idx$single_object[[rowpos]]) {
        assign(obj_to_leave,  value = oldobjs, envir = obj.environment)
      } else {
        assign(obj_to_leave,  value = oldobjs[[obj_to_leave]], envir = obj.environment)
      }
    }
  }

  if(length(objs_to_add)>1) {
    obj<-as.list(obj.environment[objs_to_add])
  } else {
    obj<-obj.environment[[objs_to_add]]
  }
  return(set_runtime_archive(obj.environment=obj.environment,
                             objectnames=objs_to_add,
                             archive_filename=archive_filename,
                             compress=compress,
                             wait_for=wait_for,
                             flag_use_tmp_storage=flag_use_tmp_storage,
                             parallel_cpus=parallel_cpus,
                             tasktpath=tasktpath))
}

#'Function parses the argument generating named vector with keys objectnames
#'
#' @param arg The argument to parse. Can be a single value, vector of lenght equal to
#'        the length of \code{objectnames} or named vector with keys from \code{objectnames}.
#' @param objectnames Objectnames that sets the basis for the intepretation of the \code{arg}
#' @param default_value Value in case \code{arg} is a named vector with some \code{objectnames}
#'        missing its values.
#' @export
parse_argument<-function(arg, objectnames, default_value) {
  argname<-substitute(arg)
  if(is.null(arg)) {
    out<-setNames(rep(default_value, length(objectnames)),objectnames)
  } else {
    if(is.null(names(arg))) {
      if(length(arg)>1) {
        if(length(arg)!=length(objectnames)) {
          stop(paste0(argname, ' should be either named vector with keys object_names, or vector with the size of object_names'))
        }
        out<-setNames(arg, objectnames)
      } else {
        out<-setNames(rep(arg, length(objectnames)), objectnames)
      }
    } else {
      if(length(setdiff(names(arg), objectnames))>0) {
        stop(paste0("The following named objects in ", argname,
                    " do not exist in objectnames: ",
                    paste0(setdiff(names(arg), objectnames), collapse=', ')))
      }

      values<-setNames(rep(default_value, length(objectnames)), objectnames)
      values[names(arg)]<-arg
      out<-values
    }
  }
  return(out)
}

