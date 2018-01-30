#' Waits for .lock file to disappear
#' @param path Path to the lock file
#' @param name Name of the lock file. Both with \code{path} are used to get
#'   the mangled connection name
#' @param timeout Timeout counted from file's creation time,
#'  after which the lock will be removed automatically.
wait.for.save<-function(path, name, timeout=30*60)
{
  conname<-mangle.connection.name(path,name)

  if(exists(conname))
  {
    con<-eval(parse(text=conname))
    parallel::mccollect(con)
  }
  wait.for.lock(path, timeout)
}


#' Saves arbitrary large object to disk using saveRDS. If compression is 'xz', then
#' the file is first saved quickly with no compression, and then the background
#' process is spawned that compresses the file in multithreaded fassion using `pxz`,
#' if the program is available.
#'
#'
#' @param obj The object to be saved
#' @param file Path to the file
#' @param compress Compression method with the same meaning as saveRDS. Default is 'xz'.
#' @param wait If set the function exits only after the object is available to read.
#' @param fn_to_run_afters_save Function that will be run after save. The function will get a single argument - path to the
#' newly created file
#' @return parallel job with the backgroud save, if wait is not 'none'
#' @export
save.large.object<-function(obj, file, compress='xz', wait_for=c('save','compress','none'),
                            flag_use_tmp_storage=FALSE, fn_to_run_after_save=NULL,
                            fn_to_run_after_compress=NULL, parallel_cpus=NULL, flag_detach=FALSE) {
  #Stage2 jest wykonywany w tle nawet wtedy, gdy wait=TRUE. Nie będzie wykonany tylko wtedy, gdy compress=FALSE
  if(!wait_for %in% c('save','compress','none')) {
    stop("wait_for must be one of 'save','compress' or 'none'")
  }
  if(is.null(parallel_cpus))
  {
    parallel_cpus<-parallel::detectCores()
  }
  if(parallel_cpus==0) {
    wait_for<-'compress'
  }
  #Funkcja kompresuje plik po szybkim zapisaniu
  save_fn_stage2<-function(obj, file_from, file_to, compress, fn_to_run_after_compress, parallel_cpus, flag_return_job) {
    pxz_wait <- flag_return_job || is.null(fn_to_run_after_save)
    if (compress=='xz')
    {
      which_pxz<-suppressWarnings(system('which pxz', intern=TRUE))
      if (length(which_pxz)>0)
      {

        if(file_from != file_to) {
          system(paste0(
            which_pxz, ' "', file_from, '" -c -T ', parallel_cpus, ' >"', file_to,
            '.tmp" && mv -f "', file_to, '.tmp" "', file_to,'" && rm "', file_from, '"'), wait=pxz_wait)
        } else {
          system(paste0(
            which_pxz, ' "', file_from, '" -c -T ', parallel_cpus, ' >"', file_to,
            '.tmp" && mv -f "', file_to, '.tmp" "', file_to,'"'), wait=pxz_wait)
        }
      } else
      {
        saveRDS(obj,file=file_to,compress=compress)
      }
    } else
    {
      saveRDS(obj,file=file_to,compress=compress)
    }
    if(!is.null(fn_to_run_after_compress)){
      fn_to_run_after_compress(file_to)
    }
  }


  #Funkcja zapisuje plik na szybko, aby jaknajszybciej oddać sterowanie.
  #Funkcja może być użyta tylko wtedy, gdy nie używamy flag_use_tmp_storage i wait=FALSE
  save_fn_stage1<-function(obj, file, fn_to_run_after_save, flag_use_tmp_storage, flag_return_job,
                           stage2fn, compress, fn_to_run_after_compress, parallel_cpus, flag_compress_async) {
    if(flag_use_tmp_storage){
      filetmp=tempfile(fileext = '.rds')
    } else {
      filetmp=file
    }
    saveRDS(obj, filetmp, compress = FALSE)
    if(!is.null(fn_to_run_after_save)){
      fn_to_run_after_save(file)
    }

    if(compress!=FALSE){
      if(flag_compress_async) {
        job<-parallel::mcparallel(
          stage2fn(obj, file_from=filetmp, file_to=file, compress=compress, fn_to_run_after_compress=fn_to_run_after_compress,
                   parallel_cpus=parallel_cpus, flag_return_job=flag_return_job),
          detached=!flag_return_job)
      } else {
        stage2fn(obj, file_from=filetmp, file_to=file, compress=compress, fn_to_run_after_compress=fn_to_run_after_compress,
                 parallel_cpus=parallel_cpus, flag_return_job=flag_return_job)
        job<-NULL
      }
    } else {
      if(!is.null(fn_to_run_after_compress)){
        fn_to_run_after_compress(file)
      }
    }
    return(job)
  }



  if(wait_for=='compress') {
    save_fn_stage1(obj=obj, file=file, fn_to_run_after_save = fn_to_run_after_save,
                   flag_use_tmp_storage = flag_use_tmp_storage, flag_return_job = !flag_detach,
                   stage2fn = save_fn_stage2, fn_to_run_after_compress=fn_to_run_after_compress, compress = compress,
                   parallel_cpus=parallel_cpus, flag_compress_async=FALSE)
    job<-NULL
  } else if (wait_for=='save') {
    job<-save_fn_stage1(obj=obj, file=file, fn_to_run_after_save = fn_to_run_after_save,
                        flag_use_tmp_storage = flag_use_tmp_storage, flag_return_job = !flag_detach,
                        stage2fn = save_fn_stage2, fn_to_run_after_compress=fn_to_run_after_compress, compress = compress,
                        parallel_cpus=parallel_cpus, flag_compress_async=TRUE)
  } else if (wait_for=='none') {
    job<-parallel::mcparallel(
      save_fn_stage1(obj=obj, file=file, fn_to_run_after_save = fn_to_run_after_save,
                     flag_use_tmp_storage = flag_use_tmp_storage, flag_return_job = !flag_detach,
                     stage2fn = save_fn_stage2, fn_to_run_after_compress=fn_to_run_after_compress, compress = compress,
                     parallel_cpus=parallel_cpus, flag_compress_async=FALSE),
      detached = wait_for=='none')
  }
  return(job)
}

#Function performs actions on one archive. There are two actions: add objects -
# - given by obj.environment, and addobjectnames, or remove objects -
# - given by removeobjectnames.
#In case of non-trivial run (the one that requires compressing the archive),
#it uses the following arguments:
#compress, wait_for, flag_use_tmp_storage and parallel_cpus
modify_runtime_archive<-function(storagepath, obj.environment, addobjectnames=character(0),
                                 removeobjectnames=character(0),
                                 archive_filename, compress='gzip', wait_for='save',
                                 flag_use_tmp_storage=FALSE, parallel_cpus=NULL) {
  if(is.null(addobjectnames)){
    addobjectnames<-character(0)
  }
  if(length(addobjectnames)==0) {
    addobjectnames<-character(0)
  } else {
    if(length(addobjectnames)==1) {
      if(is.na(addobjectnames)) {
        addobjectnames<-character(0)
      }
    }
    if('list' %in% class(addobjectnames)) {
      addobjectnames<-unlist(addobjectnames)
    }
  }

  if(length(removeobjectnames)==0) {
    removeobjectnames<-character(0)
  } else {
    if(length(removeobjectnames)==1) {
      if(is.na(removeobjectnames)) {
        removeobjectnames<-character(0)
      }
    }
    if('list' %in% class(removeobjectnames)) {
      removeobjectnames<-unlist(removeobjectnames)
    }
  }


  if(length(addobjectnames)==0 && length(removeobjectnames)==0) {
    return() #nothing to do
  }
  archivepath<-pathcat::path.cat(dirname(storagepath), archive_filename)

  if(length(intersect(removeobjectnames, addobjectnames))>0) {
    browser()
    #Makes no sense in adding and deleting the same object in one step
  }

  idx<-dplyr::filter(list_runtime_objects(storagepath), archive_filename==!!archive_filename)
  if(is.na(compress)) {
    if(nrow(idx)==0) {
      browser()
      stop("Attempt to remove a last object of the archive instead of simply deleting its file")
    }
    compress<-idx$compress[[1]]
    flag_use_tmp_storage<-idx$flag_use_tmp_storage[[1]]
  }
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

  return(set_runtime_archive(obj.environment=obj.environment,
                             objectnames=objs_to_add,
                             archive_filename=archive_filename,
                             compress=compress,
                             wait_for=wait_for,
                             flag_use_tmp_storage=flag_use_tmp_storage,
                             parallel_cpus=parallel_cpus,
                             storagepath=storagepath))
}

set_runtime_archive<-function(storagepath, obj.environment, objectnames=NULL,
                              archive_filename, compress='gzip', wait_for='save',
                              flag_use_tmp_storage=FALSE, parallel_cpus=NULL
                              ) {
  if(is.null(objectnames)){
    objectnames<-ls(obj.environment)
  }

  archivepath<-pathcat::path.cat(dirname(storagepath), archive_filename)
  hashattrname<-getOption('objectstorage.reserved_attr_for_hash')
  get_digest<-function(objectname, env) {
    hash<-calculate.object.digest(objectname, obj.environment,
                                  flag_use_attrib = TRUE, flag_add_attrib = FALSE)
    if(!is.null(attr(env[[objectname]],hashattrname))) {
      setattr(env[[objectname]], hashattrname, NULL)
    }
    return(hash)
  }
  digests<-purrr::map_chr(objectnames, calculate.object.digest, target.environment=obj.environment)
  clears<-purrr::map_lgl(objectnames, clear_digest_cache, envir=obj.environment)
  if(!all(clears)) {
    browser()
    stop(paste0("Some objects were not found in the environment!"))
  }

  # sizes<-rep(NA_real_, length(objectnames))
  # for (i in seq(1, length(objectnames))) {
  #   sizes[[i]]<-object.size(obj.environment)
  # }

  sizes<-as.numeric(purrr::map_chr(objectnames, ~object.size(get(., envir = obj.environment))))

  if(length(objectnames)==0) {
    if(file.exists(archivepath)) {
      unlink(archivepath)
      dbchunk<-empty_objectstorage()
      return(list(job=NULL, dbchunk=dbchunk))
    }
  } else {
    dbchunk<-data.table(objectnames=objectnames, digest=digests, size=sizes,
                        single_object=length(objectnames)==1,
                        archive_filename=archive_filename,
                        compress=compress, flag_use_tmp_storage=flag_use_tmp_storage)



    if(length(objectnames)>1) {
      obj<-list()
      for(objname in objectnames) {
        obj[[objname]]<-get(objname, envir = obj.environment)
      }
    } else {
      obj<-get(objectnames, envir = obj.environment)
    }
    archive_filename<-pathcat::path.cat(dirname(storagepath), archive_filename)
    job<-depwalker:::save.large.object(obj = obj, file = archive_filename, compress = compress,
                                       wait_for = wait_for, flag_use_tmp_storage = flag_use_tmp_storage,
                                       parallel_cpus = parallel_cpus, flag_detach = FALSE)
    return(list(job=job, dbchunk=dbchunk))
  }

}
