lists_to_df<-function(l, list_columns=character(0)) {
  cns<-names(l[[1]])
  nrow<-length(l)

  nas<-list()

  dt<-data.table(..delete=rep(NA, nrow))
  for(cn in cns) {
    if(cn %in% list_columns) {
      val<-list(list())
      nas[[cn]]<-NA
    } else {
      val<-l[[1]][[cn]]
      val[[1]]<-NA
      nas[[cn]]<-val
    }
    dt[[cn]]<-rep(val, nrow)
  }
  for(i in seq(1, nrow)) {
    for(cn in cns) {
      val<-l[[i]][[cn]]
      if(is.null(val)) {
        val<-nas[[cn]]
      }
      if(cn %in% list_columns) {
        val<-list(list(val))
      }
      set(dt, i, cn, val)
    }
  }
  dt[['..delete']]<-NULL
  return(dt)
}


#' Calculates object's digest
#'
#' It efficiently calculates object's digest.
#'
#' It treats \code{data.table} objects separately. Rather than invoking \code{digest::digest()}
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
#'
#Funkcja kalkuluje object.digest obiektu. Nie wkłada go do parentrecord.
#Dla obiektów typu data.frame używany jest szczególnie wydajny pamięciowo
#algorytm, który liczy digest zmienna-po-zmiennej
calculate.object.digest<-function(objectname, target.environment=NULL, flag_use_attrib=TRUE, flag_add_attrib=FALSE)
{
  if (!is.character(objectname))
    stop('Needs string parameter')

  if(is.null(target.environment)) {
    stop("target.environment is missing")
  }


  if(flag_use_attrib) {
    if(!is.null(attr(get(objectname, envir = target.environment), getOption('reserved_attr_for_hash')))) {
      return(attr(get(objectname, envir = target.environment), getOption('reserved_attr_for_hash')))
    }
  }

  #Należy usunąć nasze metadane do kalkulacji digestu, bo metadane same mogą zawierać digest i nigdy nie uzyskamy spójnych wyników
  obj<-target.environment[[objectname]]

  if (data.table::is.data.table(obj))
  {
    d<-tryCatch(parallel::mclapply(obj , function(x) digest::digest(x, algo="md5")),
                error=function(e) e)
    if ('error' %in% class(d))
    {
      d<-lapply(obj , function(x) digest::digest(x, algo="md5"))
    }
    d<-digest::digest(d[order(names(d))])
  } else {
    d<-digest::digest(obj)
  }
  if(flag_add_attrib) {
    if(objectname %in% names(target.environment)) { #It may be in the parent of the environment, and in this case we will not touch it
      data.table::setattr(target.environment[[objectname]], getOption('reserved_attr_for_hash'), d)
    }
  }
  assertDigest(d)
  return(d)
}

clear_digest_cache<-function(objectname, envir) {
  while(!identical(envir, .GlobalEnv)) {
    if(objectname %in% names(envir)) {
      setattr(envir[[objectname]], getOption('reserved_attr_for_hash'), NULL)
      return(TRUE)
    }
    envir<-parent.env(envir)
  }
  return(FALSE)
}

assertDigest<-function(digest)
{
  checkmate::assertString(digest, pattern = '^[0-9a-f]{32}$')
}
