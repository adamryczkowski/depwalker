context("Load tasks from memory")

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')
library(testthat)

test_that("Test getting task from R memory (1)", {
  testf14(tmpdir);
  #system(paste0('nemo ', tmpdir))
  m<-depwalker:::load.metadata(file.path(tmpdir, "task14"));
  env<-new.env()
  debugonce(depwalker:::is.cached.value.stale)
  m<-depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task14"),
                             target.environment = env, flag.save.in.background = FALSE)
  testthat::expect_equal(env$x, 42)
  testthat::expect_true(file.exists(file.path(tmpdir, 'testf14.tmp')))

  unlink(file.path(tmpdir, 'testf14.tmp'))
  unlink(file.path(tmpdir, 'x.rds'))
  debugonce(depwalker:::load.objects.by.metadata)
  depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task14"), objectname="x",
                          target.environment = env, flag.save.in.background = FALSE)
  testthat::expect_false(file.exists(file.path(tmpdir, 'testf14.tmp')))

})

test_that("Test getting task from R memory without CRC (1)", expect_equal({
  testf1(tmpdir);
  env<-new.env()
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  if (exists('x',envir=env))
    rm('x',envir=env);
  assign(x='x', value=1:5,envir = env);

  depwalker:::load.object(
    metadata=m,
    metadata.path=file.path(tmpdir, "task1"),
    target.environment = env,
    objectname="x",
    flag.check.object.digest = FALSE)

  env$x

}, 1:5))
