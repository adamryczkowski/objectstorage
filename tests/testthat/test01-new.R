context("New")
library(objectstorage)
library(testthat)

#source('tests/testthat/testfunctions.R')

source('testfunctions.R')

testthat::test_that("Test whether the code can create a new storage", {
  idx<-pathcat::path.cat(tmpdir, 'test_new')
  testthat::expect_false(file.exists(paste0(idx, getOption('objectstorage.index_extension'))))
  create_objectstorage(idx)
  testthat::expect_true(file.exists(paste0(idx, getOption('objectstorage.index_extension'))))
  create_objectstorage(idx)
  testthat::expect_true(file.exists(paste0(idx, getOption('objectstorage.index_extension'))))
  ans<-readRDS(paste0(idx, getOption('objectstorage.index_extension')))
  testthat::expect_true('data.frame' %in% class(ans))
  testthat::expect_true(nrow(ans)==0)
})


