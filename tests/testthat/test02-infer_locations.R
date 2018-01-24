context("Infering locations of save objects")
library(objectstorage)
library(testthat)
#source('tests/testthat/testfunctions.R')

source('testfunctions.R')

test_that("Empty run", {
  storagepath<-pathcat::path.cat(tmpdir, 'test_infer')

  ans<-infer_save_locations(storagepath, obj.environment=new.env())
  testthat::expect_true(is.null(ans))
  }
)

test_that("One object", {
  storagepath<-pathcat::path.cat(tmpdir, 'test_infer')
  env<-new.env()
  env$a<-100

#  debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env, forced_archive_paths='a.rds')
  testthat::expect_false(is.null(ans))
  ans_ref<-list('a.rds'=list(archive_filename='a.rds', objectnames='a', compress='gzip', flag_use_tmp_storage=FALSE))
  testthat::expect_equivalent(ans, ans_ref)
}
)


test_that("Two objects, default inference", {
  storagepath<-paste0(tmpdir, 'test_infer')
  env<-new.env()
  env$a<-100
  env$b<-101

#  debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  testthat::expect_false(is.null(ans))
  ans_ref<-list(
    archive_filename=ans[[1]]$archive_filename,
    objectnames=c("a", "b"),
    compress="gzip",
    flag_use_tmp_storage=FALSE)
  testthat::expect_equivalent(ans[[1]], ans_ref)
}
)

test_that("Two objects, default inference", {
  storagepath<-pathcat::path.cat(tmpdir, 'test_infer')
  env<-new.env()
  env$a<-100
  env$b<-102

#  debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  testthat::expect_false(is.null(ans))
  ans_ref<-list(
    archive_filename=ans[[1]]$archive_filename,
    objectnames=c("a", "b"),
    compress="gzip",
    flag_use_tmp_storage=FALSE)
  testthat::expect_equivalent(ans[[1]], ans_ref)
}
)

test_that("Two objects, one large, default inference", {
  storagepath<-pathcat::path.cat(tmpdir, 'test_infer')
  env<-new.env()
  env$a<-100
  env$b<-runif(2000)

 # debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  testthat::expect_false(is.null(ans))
  ans_ref<-list(
    archive_filename=ans[[1]]$archive_filename,
    objectnames=c("a", "b"),
    compress="gzip",
    flag_use_tmp_storage=FALSE)
  testthat::expect_equivalent(ans[[1]], ans_ref)
}
)

test_that("Multiple objects that share common, to test naming", {
  storagepath<-pathcat::path.cat(tmpdir, 'test_infer')

  objnames<-unique(purrr::map_chr(1:20, ~paste0(sample(c(LETTERS, letters),2), collapse=''),10))
  objsizes<-pmin(round(rexp(length(objnames),1/100000)/20), getOption('tune.threshold_objsize_for_dedicated_archive'))
  objsizes<-pmax(objsizes-96-8, 1)

  env<-new.env()
  for(i in seq_along(objnames)) {
    pname<-objnames[[i]]
    objsize<-objsizes[[i]]
    assign(pname, paste0(sample(LETTERS, objsize, replace = TRUE), collapse=''), envir = env)
  }
  assign('a', paste0(sample(LETTERS, getOption('tune.threshold_objsize_for_dedicated_archive'), replace = TRUE), collapse=''), envir = env)


#  debugonce(infer_save_locations)
  ans<-infer_save_locations(storagepath, obj.environment=env)
  testthat::expect_false(is.null(ans))
  testthat::expect_equal(length(ans), 2)
  filename<-paste0(getOption('prefix_for_automatic_dedicated_archive_names'),'a.rds')
  testthat::expect_true(filename %in% names(ans))
  testthat::expect_equivalent(ans[[filename]], list(archive_filename=filename,
                                                    objectnames='a',
                                                    compress='gzip',
                                                    flag_use_tmp_storage=FALSE))

}
)
