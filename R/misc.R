#' Transforms list of records into a data.frame.
#'
#' @param l List of records. It is assumed, that all items with the same name share the same data type
#' @param list_columns Character vector with the names of the fields that should be treated as a nested lists.
#'                     They may contain heterogenous contents or simply be atomic types with length more than 1.
#' @return Returns a tibble
#' @export
lists_to_df<-function(l, list_columns=character(0), unnamed_default=NA_character_) {
  classes<-purrr::map(l, class)
  idx_lists<-which(purrr::map_lgl(classes, ~'list' %in% .))
  cns<-unique(unlist(purrr::map(l[idx_lists], names)))
  unnamed_count<-max(purrr::map_int(l, ~length(.)-length(names(.))))
  if(unnamed_count>0) {
    unnamed_cns<-paste0('.col_', seq_len(unnamed_count))
  } else {
    unnamed_cns<-character(0)
  }
  nrow<-length(l)

  nas<-list()

  dt<-data.table(..delete=rep(NA, nrow))
  for(cn in cns) {
    if(cn %in% list_columns) {
      val<-list(list())
      nas[[cn]]<-NA
    } else {
      val<-l[[1]][[cn]]
      if('character' %in% class(val)) {
        val[[1]]<-NA_character_
      } else {
        val[[1]]<-NA
      }
      nas[[cn]]<-val
    }
    dt[[cn]]<-rep(val, nrow)
  }
  for(cn in unnamed_cns) {
    nas[[cn]]<-unnamed_default
    dt[[cn]]<-rep(unnamed_default, nrow)
  }

  for(i in seq(1, nrow)) {
    for(cn in cns) {
      val<-l[[i]][[cn]]
      if(is.null(val)) {
        val<-nas[[cn]]
      }
      if(cn %in% list_columns) {
        val<-list(list(val))
      } else {
      }
      tryCatch(
        set(dt, i, cn, val),
        error=function(e) {dt[[cn]][[i]]<<-val}
      )
    }
    if(unnamed_count>0) {
      item<-l[[i]]
      ucnt<-length(item) - length(names(item))
      unames<-names(item)
      if(is.null(unames)) {
        unames<-rep('', length(item))
      }
      un<-1
      for(j in seq_along(item)) {
        if(unames[[j]]=='') {
          val<-item[[j]]
          set(dt, i, unnamed_cns[[un]], val)
          un<-un+1
        }
      }
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
#' @export
calculate.object.digest<-function(objectname, target.environment=NULL, flag_use_attrib=TRUE, flag_add_attrib=FALSE)
{
  if (!is.character(objectname))
    stop('Needs string parameter')

  if(is.null(target.environment)) {
    stop("target.environment is missing")
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
      setattr(envir[[objectname]], getOption('objectstorage.reserved_attr_for_hash'), NULL)
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
