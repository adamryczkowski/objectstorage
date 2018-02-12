#' Halts execution until the lock is released, or lock expires.
#'
#' @param path Path to the lock file
#' @param timeout Wall time after which the lock will be considered open regardless if someone opened it
#' @export
wait.for.lock<-function(path, timeout=NULL)
{
  if(is.null(timeout)) {
    timeout<-getOption('objectstorage.default_lock_time')
  }
  lockfile<-get_lock_file(path)
  if (file.exists(lockfile))
  {
    t<-as.numeric(file.mtime(lockfile))
    while (file.exists(lockfile))
    {
      if (t-as.numeric(Sys.time())>timeout)
      {
        release.lock.file(path)
        break;
      }
      Sys.sleep(1)
    }
  }
}

#' Tests if the lock is locked
#'
#' @param path Path to the lock file
#' @param timeout Wall time after which the lock will be considered open regardless if someone opened it
#' @export
lock.exists<-function(path, timeout=NULL) {
  lockfile<-get_lock_file(path)
  if(is.null(timeout)) {
    timeout<-getOption('objectstorage.default_lock_time')
  }
  if (file.exists(lockfile))
  {
    t<-as.numeric(file.mtime(lockfile))
    if (t-as.numeric(Sys.time())>timeout)
    {
      return(FALSE)
    } else {
      return(TRUE)
    }
  } else {
    return(FALSE)
  }
}

#' Creates lock file on a given path. Lock file is just a 0-length file.
#'
#' The file will have an extension getOption('objectstorage.lock_extension')
#'
#' @param path Path to the lock file
#' @param timeout Wall time after which the lock will be considered open regardless if someone opened it
#' @export
create.lock.file<-function(path, timeout=NULL)
{
  if(is.null(timeout)) {
    timeout<-getOption('objectstorage.default_lock_time')
  }
  wait.for.lock(path, timeout)
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  write.table(data.frame(), file=get_lock_file(path), col.names=FALSE)
}

#' Removes the lock file.
#' @param path Path to the lock file
#' @export
release.lock.file<-function(path)
{
  filename<-get_lock_file(path)
  if (file.exists(filename))
    unlink(filename)
}


get_lock_file<-function(path) {
  ext<-getOption('objectstorage.lock_extension')
  ext2<-stringr::str_replace(ext, pattern=stringr::fixed('.'), replacement = '\\.')
  if(!stringr::str_detect(path, stringr::regex(paste0(ext2, '$')))) {
    path<-paste0(path, ext)
  }
  return(path)
}
