library(testthat)
context("Saving objects")
library(objectstorage)
#source('tests/testthat/testfunctions.R')

source('testfunctions.R')

test_that("Save simple forced object", {
#  library(data.table)
  #  source('tests/testthat/testfunctions.R')
  storagepath<-pathcat::path.cat(tmpdir, 'add_1_forced')
  env<-new.env()
  env$a<-100

  ans<-infer_save_locations(storagepath, obj.environment=env, forced_archive_paths='a.rds')

#  debugonce(set_runtime_archive)
#  debugonce(add_runtime_objects_internal)
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)
  sp<-list_runtime_objects(storagepath = storagepath)
  testthat::expect_equivalent(as.list(sp), list(objectnames='a', digest=digest::digest(env$a),
                                                size=as.numeric(object.size(env$a)),
                                                archive_filename='a.rds', single_object=TRUE,
                                                compress='gzip', flag_use_tmp_storage=FALSE))
  env2<-new.env()
  expect_true(load_objects(storagepath = storagepath, objectnames = 'a', env2, flag_double_check_digest = TRUE))
}
)

test_that("Save simple forced object", {
  #  source('tests/testthat/testfunctions.R')
  storagepath<-pathcat::path.cat(tmpdir, 'add_1_default')
  env<-new.env()
  env$a<-100

  ans<-infer_save_locations(storagepath, obj.environment=env)

  #  debugonce(set_runtime_archive)
  #  debugonce(add_runtime_objects_internal)
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)
  sp<-list_runtime_objects(storagepath = storagepath)
  testthat::expect_equivalent(as.list(sp), list(objectnames='a', digest=digest::digest(env$a),
                                                size=as.numeric(object.size(env$a)),
                                                archive_filename='add_1_default.rda', single_object=TRUE,
                                                compress='gzip', flag_use_tmp_storage=FALSE))

  env2<-new.env()
  #debugonce(load_objects)
  testthat::expect_true(load_objects(storagepath = storagepath, objectnames = 'a', env2, flag_double_check_digest = TRUE))

}
)


test_that("Save two objects, default inference", {
  storagepath<-pathcat::path.cat(tmpdir, 'add_2_default')
  env<-new.env()
  env$a<-100
  env$b<-101

#  debugonce(add_runtime_objects_internal)
#  debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)
  sp<-list_runtime_objects(storagepath = storagepath)
  #  debugonce(add_runtime_objects_internal)

  testthat::expect_equivalent(as.list(sp), list(objectnames=c('a', 'b'), digest=c(digest::digest(env$a), digest::digest(env$b)),
                                                size=as.numeric(c(object.size(env$a), object.size(env$b))),
                                                archive_filename=rep('add_2_default.rda',2), single_object=rep(FALSE,2),
                                                compress=c('gzip', 'gzip'), flag_use_tmp_storage=c(FALSE,FALSE)))

  env2<-new.env()
#  debugonce(load_objects)
  testthat::expect_true(all(load_objects(storagepath = storagepath, objectnames = c('a', 'b'), env2, flag_double_check_digest = TRUE)))
  testthat::expect_equal(env2$a, env$a)
  testthat::expect_equal(env2$b, env$b)

  env3<-new.env()
  #debugonce(load_objects)
  testthat::expect_true(load_objects(storagepath = storagepath, objectnames = 'a',target_environment =  env3, flag_double_check_digest = TRUE))
  load_objects(storagepath = storagepath, objectnames = 'a',target_environment =  env3, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(as.list(env3), list(a=100))
}
)

test_that("Two objects, one large, default inference", {
  storagepath<-pathcat::path.cat(tmpdir, 'add_2_large')
  env<-new.env()
  env$a<-100
  env$b<-runif(20000)

  # debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  testthat::expect_false(is.null(ans))
  #debugonce(add_runtime_objects_internal)
  objectstorage:::add_runtime_objects_internal(storagepath = storagepath, obj.environment = env, archives_list = ans, parallel_cpus = 0)

  env2<-new.env()
  #debugonce(load_objects)
  testthat::expect_true(all(load_objects(storagepath = storagepath, objectnames = c('a', 'b'), env2, flag_double_check_digest = TRUE)))
  testthat::expect_equivalent(env, env2)
}
)

test_that("Multiple objects that share common, to test naming", {
  storagepath<-pathcat::path.cat(tmpdir, 'test_infer')

  objnames<-unique(purrr::map_chr(1:20, ~paste0(sample(c(LETTERS, letters),2), collapse=''),10))
  objsizes<-pmin(round(rexp(length(objnames),1/100000)/20), getOption('objectstorage.tune_threshold_objsize_for_dedicated_archive'))
  objsizes<-pmax(objsizes-96-8, 1)

  env<-new.env()
  for(i in seq_along(objnames)) {
    pname<-objnames[[i]]
    objsize<-objsizes[[i]]
    assign(pname, paste0(sample(LETTERS, objsize, replace = TRUE), collapse=''), envir = env)
  }
  assign('a', paste0(sample(LETTERS, getOption('objectstorage.tune_threshold_objsize_for_dedicated_archive'), replace = TRUE), collapse=''), envir = env)


  #  debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)


  modify_objects(storagepath = storagepath, obj.environment = env, parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath = storagepath, target_environment = env2, objectnames = ls(env), flag_double_check_digest = TRUE)
  expect_equivalent(env2 ,env)
}
)

