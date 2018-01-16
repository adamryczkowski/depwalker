context("Load tasks from memory")

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')

test_that("Test getting task from R memory (1)", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  if (exists('x',envir=.GlobalEnv))
    rm('x',envir=.GlobalEnv);
  depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task1"));
  depwalker:::get.object(metadata=m, metadata.path=file.path(tmpdir, "task1"), objectname="x")
}, 1:10))

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
