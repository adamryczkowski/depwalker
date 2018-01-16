context("Correct handling of external changes to the source codes")

#source('tests/testthat/testfunctions.R')
source('testfunctions.R')

test_that("External change to the main source code (1)", {
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

test_that("External change to the secondary source code and update of the test and cache afterwards (14)", {
  tmpfile =  file.path(tempdir(), 'testf14.tmp')

  unlink(tmpfile)
  m<-testf14(tmpdir)
  #  debugonce(depwalker:::make.sure.metadata.is.saved)
  #  debugonce(depwalker:::join.metadatas)
  m<-depwalker:::make.sure.metadata.is.saved(m)
  depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_true(file.exists(tmpfile))
  unlink(tmpfile)
  expect_true(exists('x', envir = .GlobalEnv))
  rm(x, envir = .GlobalEnv)

  file<-depwalker::get.codepath(m, path = 'aux.R')
  code<-'x<-43'
  writeLines(code, file)
  x<-depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_equal(x,43)
  expect_true(file.exists(tmpfile))
  unlink(tmpfile)

  x<-depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_false(file.exists(tmpfile))
  unlink(tmpfile)

  rm(x, envir = .GlobalEnv)
  x<-depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_false(file.exists(tmpfile))
  unlink(tmpfile)

})

test_that("External change to the secondary binary code and update of the test and cache afterwards (16)", {
  tmpfile =  file.path(tempdir(), 'customdep.bin')

  unlink(tmpfile)
  m<-testf16(tmpdir)
  #  debugonce(depwalker:::make.sure.metadata.is.saved)
  #  debugonce(depwalker:::join.metadatas)
  m<-depwalker:::make.sure.metadata.is.saved(m)
  depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_true(file.exists(tmpfile))
  unlink(tmpfile)
  expect_true(exists('x', envir = .GlobalEnv))
  expect_equal(x, '5c1679f8')
  rm(x, envir = .GlobalEnv)

  towrite<-as.raw(rep(0,16384))
  towrite[3672]<-as.raw(43) #Other than 42 @ 3673
  writeBin(object=towrite,con=tmpfile)

  x<-depwalker:::get.object(metadata=m,flag.save.in.background = FALSE)
  expect_equal(x, 'bd29f247')
  expect_true(file.exists(tmpfile))
  unlink(tmpfile)

  expect_error(depwalker:::get.object(metadata=m,flag.save.in.background = FALSE),regexp = 'File does not exist')
  expect_false(file.exists(tmpfile))
})
