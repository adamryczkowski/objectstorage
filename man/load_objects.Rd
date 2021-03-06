% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/objectstorage.R
\name{load_objects}
\alias{load_objects}
\title{Main function to load the objects into the given environment}
\usage{
load_objects(storagepath, objectnames = NULL,
  target_environment = rlang::caller_env(),
  flag_double_check_digest = FALSE, aliasnames = NULL)
}
\arguments{
\item{storagepath}{Path to the storage metadata}

\item{objectnames}{Objectnames to extract}

\item{target_environment}{Target environment where to put the objects}

\item{flag_double_check_digest}{Calculate the digest of the extracted objects and check it against the metadata}

\item{aliasnames}{Optional vector of the same length as \code{objectnames} with new names for the extracted objects.}
}
\value{
Logical vector, one for each loaded object. \code{TRUE} means that
        load was successfull, \code{FALSE} otherwise.
}
\description{
Main function to load the objects into the given environment
}
