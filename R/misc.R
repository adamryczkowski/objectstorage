#' Calculates object's digest
#'
#' It efficiently calculates object's digest.
#'
#' It treats \code{data.frame} objects separately. Rather than invoking \code{digest::digest()}
#' on them directly, it first splits the object into individual columns, sorts them,
#' calculates the
#' digest on each column separately (trying to do that in parallel), and merges the
#' results into one string, that is digested again.
#'
#' @param object The name of the object to calculate digest of. Object must exist in
#'   \code{.GlobalEnv}.
#'
#' @return Lowercase MD5 string with the digest of the object
#'
#' @export
calculate.object.digest<-function(objectname, target.environment=NULL, flag_use_attrib=TRUE, flag_add_attrib=FALSE)
{
  if (!is.character(objectname))
    stop('Needs string parameter')

  if(is.null(target.environment)) {
    stop("target.environment is missing")
  }
  if(!'environment' %in% class(target.environment)) {
    target.environment<-as.environment(target.environment)
  }
  if(!is.null(attr(get(objectname, envir = target.environment), getOption('objectstorage.reserved_attr_for_hash')))) {
    if(flag_use_attrib) {
      return(attr(get(objectname, envir = target.environment), getOption('objectstorage.reserved_attr_for_hash')))
    } else {
      data.table::setattr(get(objectname, envir = target.environment), getOption('objectstorage.reserved_attr_for_hash'), NULL)
    }
  }


  #Należy usunąć nasze metadane do kalkulacji digestu, bo metadane same mogą zawierać digest i nigdy nie uzyskamy spójnych wyników
  obj<-get(objectname, envir = target.environment)

  if ('data.frame' %in% class(obj))
  {
    d<-tryCatch(parallel::mclapply(obj , function(x) mydigest(x)),
                error=function(e) e)
    if ('error' %in% class(d))
    {
      d<-lapply(obj , function(x) mydigest(x))
    }
    d<-mydigest(d[order(names(d))])
  } else {
    d<-mydigest(obj)
  }
  if(flag_add_attrib) {
    if(objectname %in% ls(envir = target.environment)) { #It may be in the parent of the environment, and in this case we will not touch it
      data.table::setattr(get(objectname, envir = target.environment), getOption('objectstorage.reserved_attr_for_hash'), d)
    }
  }
  assertDigest(d)
  return(d)
}

clear_digest_cache<-function(objectname, envir) {
  while(!identical(envir, .GlobalEnv)) {
    if(objectname %in% names(envir)) {
      data.table::setattr(envir[[objectname]], getOption('objectstorage.reserved_attr_for_hash'), NULL)
      return(TRUE)
    }
    envir<-parent.env(envir)
  }
  return(objectname %in% names(envir))
}

assertDigest<-function(digest)
{
  checkmate::assertString(digest, pattern = '^[0-9a-f]{32}$')
}


mydigest<-function(object, algo='md5', ...) {
  library(future)
  #future::plan(future::multiprocess)
  fname <-tempfile()
  producer %<-% {
    mystream <- file(fname, open="wb")
    writeBin(object, endian="little", con=mystream)
    close(mystream) # sends signal to the consumer (digester)
  }

  md5 <- digest::digest(fname, file = TRUE, algo = algo, ...)
}
