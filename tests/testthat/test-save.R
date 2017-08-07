context("Correct hanlding of concurrent writes/saves")

source('testfunctions.R')

test_that("Saving to non-existent directory (1)", {
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

test_that("Testing saving with pxz compression (1)", {
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

test_that("Testing saving with default R compression (13)", {
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

test_that("Saving more than one object", {
  #TODO:
  #Save two objects at once with code that does some side-effect.
  #Then see if they get loaded from a) memory, b) disk without the code being run
})
