context("Correct handling of external changes to the source codes")

source('testfunctions.R')

test_that("External change to the main source code", {
  m<-testf1(tmpdir)
  #  debugonce(depwalker:::make.sure.metadata.is.saved)
  #  debugonce(depwalker:::join.metadatas)
  m<-depwalker:::make.sure.metadata.is.saved(m)
  depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_true(exists('x', envir = .GlobalEnv))
  rm(x, envir = .GlobalEnv)

  file<-depwalker::get.codepath(m)
  code<-'x<-5:15'
  writeLines(code, file)
  x<-depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_equal(x,5:15)
})

test_that("External change to the secondary source code", {
  m<-testf14(tmpdir)
  #  debugonce(depwalker:::make.sure.metadata.is.saved)
  #  debugonce(depwalker:::join.metadatas)
  m<-depwalker:::make.sure.metadata.is.saved(m)
  depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_true(exists('x', envir = .GlobalEnv))
  rm(x, envir = .GlobalEnv)

  file<-depwalker::get.codepath(m, path = 'aux.R')
  code<-'x<-43'
  writeLines(code, file)
  x<-depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_equal(x,43)
})

