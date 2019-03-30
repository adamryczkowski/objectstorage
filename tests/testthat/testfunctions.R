sample(c(letters, LETTERS, 0:9), 15)
tmpdir<-file.path(tempfile('objectstorage.test.'), paste0(sample(c(letters, LETTERS, 0:9), 15),collapse=''))
dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE);
#system(paste0('nemo "', tmpdir, '"'))


simple_storage<-function(nazwa='simple_storage') {
  storagepath<-pathcat::path.cat(tmpdir, nazwa)
  env<-new.env()
  env$a<-100

  ans<-infer_save_locations(storagepath, obj.environment=env)
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)
  sp<-list_runtime_objects(storagepath = storagepath)
  testthat::expect_equivalent(as.list(sp), list(objectnames='a', digest=digest::digest(env$a),
                                                size=as.numeric(object.size(env$a)),
                                                archive_filename=paste0(nazwa, '.rda'), single_object=TRUE,
                                                compres='gzip', flag_use_tmp_storage=FALSE))

  env2<-new.env()
  testthat::expect_true(load_objects(storagepath = storagepath, objectnames = 'a', env2, flag_double_check_digest = TRUE))
  return(storagepath)
}

double_storage<-function(nazwa="double_storage") {
  storagepath<-pathcat::path.cat(tmpdir, nazwa)
  env<-new.env()
  env$a<-100
  env$b<-101

  ans<-infer_save_locations(storagepath, obj.environment=env)
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)
  sp<-list_runtime_objects(storagepath = storagepath)

  testthat::expect_equivalent(as.list(sp), list(objectnames=c('a', 'b'), digest=c(digest::digest(env$a), digest::digest(env$b)),
                                                size=as.numeric(c(object.size(env$a), object.size(env$b))),
                                                archive_filename=rep(paste0(nazwa, '.rda'),2), single_object=rep(FALSE,2),
                                                compress=c('gzip', 'gzip'), flag_use_tmp_storage=c(FALSE,FALSE)))

  env2<-new.env()
  testthat::expect_true(all(load_objects(storagepath = storagepath, objectnames = c('a', 'b'), env2, flag_double_check_digest = TRUE)))
  testthat::expect_equal(env2$a, env$a)
  testthat::expect_equal(env2$b, env$b)

  env3<-new.env()
  testthat::expect_true(all(load_objects(storagepath = storagepath, objectnames = 'a',target_environment =  env3, flag_double_check_digest = TRUE)))
  load_objects(storagepath = storagepath, objectnames = 'a',target_environment =  env3, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(as.list(env3), list(a=100))
  return(storagepath)
}

double_storage_ex<-function(nazwa='double_storage_ex'){
  storagepath<-pathcat::path.cat(tmpdir, nazwa)
  env<-new.env()
  env$a<-100
  env$b<-runif(20000)

  # debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  testthat::expect_false(is.null(ans))
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)

  env2<-new.env()
#  debugonce(load_objects)
  testthat::expect_true(all(load_objects(storagepath = storagepath, objectnames = c('a', 'b'), env2, flag_double_check_digest = TRUE)))
  testthat::expect_equivalent(env, env2)
  return(storagepath)
}

