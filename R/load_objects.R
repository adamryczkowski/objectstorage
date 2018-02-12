#loads objects from the archive into the envir.
#archive path must be absolute.
load_objects_from_archive<-function(archive_path, objectnames, envir, flag_multi_archive) {
  if(flag_multi_archive) {
    empty_env<-new.env()
    assign('a', value = readRDS(archive_path), envir = envir)
    for(objname in objectnames) {
      assign(objname, value=empty_env$a[[objname]], envir=envir)
    }
  } else {
    if(length(objectnames)!=1) {
      browser() #Ojbectnames must equal 1 in single object archive
    }
    assign(objectnames, value = readRDS(archive_path), envir = envir)
  }
}
