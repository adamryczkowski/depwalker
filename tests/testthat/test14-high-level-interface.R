context("Test of high level interface")
library(testthat)

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')

test_that("Simple test of high level interface", {
  code<-"t19<-exists('ans1') + exists('ans2')";
  M<-depwalker:::ObjectMetadata$new(code = code, metadata.path = file.path(tmpdir, "task19"))
  M$add_objectrecord(name = 't19')
  m2<-testf8(tmpdir); #More than 1 object exported
  M$add_parent(parent = m2)
  M$save()

  a<-readLines(paste0(M$path, '.meta.yaml'))

  unlink(paste0(M$path, '.meta.yaml'))
  M

  m<-testf19(tmpdir)
  a_orig<-readLines(paste0(M$path, '.meta.yaml'))

  testthat::expect_true(all(a==a_orig))
  #system(paste0('nemo ', dirname(M$path)))
})
