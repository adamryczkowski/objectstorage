context("Testing digests")

source('testfunctions.R')
library(objectstorage)
library(testthat)
#source('tests/testthat/testfunctions.R')


test_that("Updating all objects in multi-object archive", {
  #  debugonce(double_storage)
  storagepath<-double_storage('update_2_obj_all')

  digests<-get_object_digest(storagepath = storagepath, objectnames = c('b', 'a'))
  expect_identical(digests, c("16e8b34f96a712da6a2c777162d0d032", "d344558826c683dbadec305ed64365f1"))

  env<-new.env()
  load_objects(storagepath, objectnames = c('a','b'), target.environment = env, flag_double_check_digest = TRUE)

  digest1<-get_full_digest(storagepath = storagepath)

  testthat::expect_equal(digest1, '8d74a6c5dc570a0f9a22832f09bfd14c')

#  debugonce(modify_runtime_archive)
#  debugonce(modify_runtime_objects)
  modify_runtime_objects(storagepath = storagepath, obj.environment = env, objects_to_add = c('a'), parallel_cpus = 0,
                         forced_archive_paths = 'a.rds')

  digest1<-get_full_digest(storagepath = storagepath)

  testthat::expect_equal(digest1, '8d74a6c5dc570a0f9a22832f09bfd14c')

  idx<-list_runtime_objects(storagepath = storagepath)
  testthat::expect_equal(length(unique(idx$archive_filename)),2)
})

