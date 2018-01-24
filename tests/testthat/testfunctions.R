sample(c(letters, LETTERS, 0:9), 15)
tmpdir<-file.path(tempfile('objectstorage.test.'), paste0(sample(c(letters, LETTERS, 0:9), 15),collapse=''))
dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE);
system(paste0('nemo "', tmpdir, '"'))
