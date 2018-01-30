context("Modification of object storage")

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')
library(testthat)

test_that("Updating single object", {
  storagepath<-simple_storage('update_1_obj')
  env<-new.env()
  env$a<-'bla'
#  debugonce(modify_runtime_objects)
#  debugonce(add_runtime_objects_internal)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = 'a', parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = 'a', target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(env2, env)
})

test_that("Updating single object in multi-object archive", {
#  debugonce(double_storage)
  storagepath<-double_storage('update_2_obj')

  env<-new.env()
  env$a<-'bla'
  #  debugonce(modify_runtime_objects)
  #  debugonce(add_runtime_objects_internal)
  #  debugonce(modify_runtime_archive)
#  debugonce(set_runtime_archive)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = 'a', parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = 'a', target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(env2, env)
})

test_that("Updating all objects in multi-object archive", {
  #  debugonce(double_storage)
  storagepath<-double_storage('update_2_obj_all')

  env<-new.env()
  env$a<-'bla'
  env$b<-'blu'
  #  debugonce(modify_runtime_objects)
  #  debugonce(add_runtime_objects_internal)
  #  debugonce(modify_runtime_archive)
  #  debugonce(set_runtime_archive)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('a'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(as.list(env2), list(a=env$a, b=101))

  env$a<-'bli'
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('a','b'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(env2, env)

  env$a<-'blx'
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('a','b'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(env2, env)

})

test_that("Updating small object in multi-object split archive", {
  #  debugonce(double_storage)
  storagepath<-double_storage_ex('update_2_obj_ex_all')

  ref_env<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = ref_env, flag_double_check_digest = TRUE)

  env<-new.env()
  env$a<-'bla'
  env$b<-'blu'



  #  debugonce(modify_runtime_objects)
  #  debugonce(add_runtime_objects_internal)
  # debugonce(modify_runtime_archive)
  #  debugonce(set_runtime_archive)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('a'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(as.list(env2), list(a=env$a, b=ref_env$b))

  env$b<-'bli'
  #debugonce(modify_runtime_objects)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('b'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(env2, env)
})

test_that("Updating large object in multi-object split archive", {
  #  debugonce(double_storage)
  storagepath<-double_storage_ex('update_2_obj_ex_all')

  ref_env<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = ref_env, flag_double_check_digest = TRUE)

  env<-new.env()
  env$a<-'bla'
  env$b<-'blu'



  #  debugonce(modify_runtime_objects)
  #  debugonce(add_runtime_objects_internal)
  # debugonce(modify_runtime_archive)
  #  debugonce(set_runtime_archive)
  #debugonce(modify_runtime_objects)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('b'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(as.list(env2), list(a=ref_env$a, b=env$b))

})


test_that("Updating two objects in multi-object archive", {
  #  debugonce(double_storage)
  storagepath<-double_storage_ex('update_2s_obj_ex_all')

  ref_env<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = ref_env, flag_double_check_digest = TRUE)

  env<-new.env()
  env$a<-'bla'
  env$b<-'blu'



  #  debugonce(modify_runtime_objects)
  #  debugonce(add_runtime_objects_internal)
  # debugonce(modify_runtime_archive)
  #  debugonce(set_runtime_archive)
  #debugonce(modify_runtime_objects)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('a', 'b'), parallel_cpus = 0)
  env2<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env2, flag_double_check_digest = TRUE)
  testthat::expect_equivalent(as.list(env2), list(a=env$a, b=env$b))
})

