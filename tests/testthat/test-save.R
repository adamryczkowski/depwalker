context("Correct hanlding of concurrent writes/saves")

source('testfunctions.R')

test_that("Saving to non-existent directory", {
  m<-testf1(tmpdir)
  m$objectrecords[[1]]$path<-'out/x'
#  debugonce(depwalker:::make.sure.metadata.is.saved)
#  debugonce(depwalker:::join.metadatas)
  depwalker:::make.sure.metadata.is.saved(m)
  path<-file.path(tmpdir,'out')
  if (file.exists(path))
  {
    unlink(path)
  }
  depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  if (!dir.exists(path))
  {
    stop("The directory was not created!")
  }
  a<-readRDS(file.path(path,'x.rds'))
  testthat::expect_equal(sum(a), 55)
})

test_that("Testing saving with pxz compression", {
  m<-testf1(tmpdir)
  m$objectrecords[[1]]$compress<-'xz'
  depwalker:::make.sure.metadata.is.saved(m)
  path<-file.path(tmpdir,'out')
  if (file.exists(path))
  {
    unlink(path)
  }
  depwalker:::get.object(metadata=m)
  if (!dir.exists(path))
  {
    stop("The directory was not created!")
  }
  a<-readRDS(file.path(path,'x.rds'))
  testthat::expect_equal(sum(a), 55)
})

test_that("Testing saving with default R compression", {
  unlink(file.path(tmpdir,'*.rds'))
  m<-testf13(tmpdir)
  m$objectrecords[[1]]$compress<-'gzip'
  depwalker:::make.sure.metadata.is.saved(m)
#  debugonce(depwalker:::save.object)
  if (is.null(depwalker:::load.object(metadata=m, flag.save.in.background=FALSE)))
    stop("Error executing gzip task")
  filesize<-file.size(file.path(tmpdir,'data.rds'))
  rm(data, envir = .GlobalEnv)
  m$objectrecords[[1]]$compress<-'xz'
  depwalker:::make.sure.metadata.is.saved(m)
  unlink(file.path(tmpdir,'data.rds'))
  depwalker:::load.object(metadata=m, flag.save.in.background = FALSE)
  filesize2<-file.size(file.path(tmpdir,'data.rds'))
  expect_true(filesize>filesize2)
})
