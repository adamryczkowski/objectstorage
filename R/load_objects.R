#loads objects from the archive into the envir.
#archive path must be absolute.
load_objects_from_archive<-function(archive_path, objectnames, aliasnames, envir, flag_multi_archive) {
  if(flag_multi_archive) {
    empty_env<-new.env()
    assign('a', value = readRDS(archive_path), envir = envir)
    for(i in seq(length(objectnames))) {
      assign(aliasnames[[i]], value=empty_env$a[[ objectnames[[i]] ]], envir=envir)
    }
  } else {
    if(length(objectnames)!=1) {
      browser() #Ojbectnames must equal 1 in single object archive
    }
    assign(aliasnames, value = readRDS(archive_path), envir = envir)
  }
}
