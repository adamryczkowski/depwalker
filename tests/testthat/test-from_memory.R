context("Load tasks from memory")

source('testfunctions.R')

test_that("Test getting task from R memory", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  if (exists('x',envir=.GlobalEnv))
    rm('x',envir=.GlobalEnv);
  depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task1"));
  depwalker:::get.object(metadata=m, metadata.path=file.path(tmpdir, "task1"), objectname="x")
}, 1:10))

test_that("Test getting task from R memory without CRC", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  if (exists('x',envir=.GlobalEnv))
    rm('x',envir=.GlobalEnv);
  assign(x='x', value=1:5,envir = .GlobalEnv);
  depwalker:::get.object(
    metadata=m,
    metadata.path=file.path(tmpdir, "task1"),
    objectname="x",
    flag.check.object.digest = FALSE)
}, 1:5))
