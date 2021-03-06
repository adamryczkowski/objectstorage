% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/objectstorage.R
\name{modify_objects}
\alias{modify_objects}
\title{Adds or removes objects in the storage.}
\usage{
modify_objects(storagepath, obj.environment = NULL,
  objects_to_add = NULL, objects_to_remove = character(0),
  flag_forced_save_filenames = FALSE, flag_use_tmp_storage = FALSE,
  forced_archive_paths = NA, compress = "gzip",
  large_archive_prefix = NULL, locktimeout = NULL, wait_for = "save",
  parallel_cpus = NULL)
}
\arguments{
\item{storagepath}{Path with the storage.}

\item{obj.environment}{Environment or named list with the objects. Defaults to the current environment.}

\item{flag_forced_save_filenames}{Controls, whether force a particular object in its own dedicated archive.
Value can be either single boolean, or vector of booleans with the same size as
\code{addobjectnames}, or named boolean vector with keys values of \code{addobjectnames}. In the latter case,
non-mentioned objects will be assumed value \code{FALSE} (i.e. not forced filename).}

\item{forced_archive_paths}{Optional character vector with custom paths to the archives. Can be a single character object,
vector with the size of \code{objects_to_add} or named vector with keys from \code{objects_to_add}.}

\item{compress}{Controls the compression of the archive. It is important to realize, that if the archive
had already contained some objects prior to modifying it and the modification would not remove the objects,
those objects will be re-compressed with the \code{compress} compression, since the archive will effectively
be re-added. Supports 3 calues: \code{none}, \code{gzip} and \code{xz}.
Value can be either single character, or vector of characters with the same size as
\code{addobjectnames}, or named character vector with keys values of \code{addobjectnames}. In the latter case,
non-mentioned objects will be assumed value \code{gzip}.}

\item{large_archive_prefix}{If set, all new archives for large objects will be saved with this prefix, otherwise in the
\code{dirname(storagepath)}.
\code{storagepath}. It is up to the user to make sure this directory is empty and no file name conflicts will
arise.}

\item{addobjectnames}{Character vector with the names of the objects to add. Defaults to all objects in the
\code{obj.environment}.}

\item{removeobjectnames}{Character vector with the names of the objects to remove. Cannot contain objects
listed in \code{addobjectnames}.}
}
\value{
Returns `data.frame` with the following columns:
\describe{
\item{\strong{objectname}}{Name of the stored object. This is a primary key.}
\item{\strong{digest}}{String with the digest of the object.}
\item{\strong{size}}{Numeric value with the size of the stored object.}
\item{\strong{forced_archive_paths}}{Path where the object is stored absolute or relative to the storage path.}
\item{\strong{single_object}}{Logical. \code{TRUE} if the archive contain only this one object. Otherwise
archive contains named list of objects.}
\item{\strong{compress}}{Type of compression used to store this individual object}
}
}
\description{
Adds or removes objects in the storage.
}
