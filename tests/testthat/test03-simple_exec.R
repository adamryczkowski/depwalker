context("Execution of simple task")
library(testthat)
source('testfunctions.R')
#source('tests/testthat/testfunctions.R')

test_that("Run simple task (1)", expect_equal({
  m<-create_metadata('simple_task')
  m<-add_source_file(metadata = m, code = 'x<-a+10')
  m<-add_inputobject(m, objectname = 'a', object = 12)
  m<-add_objectrecord(m, name='x')
  m<-add_library_entry_simple(m, 'Hmisc')
  debugonce(depwalker:::release_lock)
  m<-make_sure_metadata_is_saved(m, path = '/tmp/task1')

  #debugonce(depwalker:::is_cached_value_stale)
  depwalker:::is_cached_value_stale(m)


  testf1(tmpdir);
  #system(paste0('nemo ', tmpdir))
  m<-load_metadata(file.path('/tmp/task1', "simple_task"))




  if (exists('x',envir=.GlobalEnv))
    rm('x',envir=.GlobalEnv)
  depwalker:::get.object(metadata=m,
                         metadata.path=file.path(tmpdir, "task1"),
                         objectname="x",
                         flag.save.in.background = FALSE)
},1:10))

test_that("Re-run simple task from memory (1)", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task1"));
  depwalker:::get.object(metadata=m,
                         metadata.path=file.path(tmpdir, "task1"),
                         objectname="x",
                         flag.save.in.background = FALSE)
},1:10))

test_that("Re-run simple task from disk (1)", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task1"));
  if (exists('x',envir=.GlobalEnv))
    rm('x',envir=.GlobalEnv)
  depwalker:::get.object(metadata=m,
                         metadata.path=file.path(tmpdir, "task1"),
                         objectname="x",
                         flag.save.in.background = FALSE)
},1:10))

test_that("Execute simple task with multiple outputs (5)", expect_equal({
  testf5(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task5"));
  if (exists('a2',envir=.GlobalEnv))
    rm('a2',envir=.GlobalEnv)
  depwalker:::get.object(metadata = m,
                         metadata.path = file.path(tmpdir, "task5"),
                         objectname = "a2",
                         flag.save.in.background = FALSE)
}, 23))

test_that("Test for correct custom script directory (15)", {
  dir<-getwd()
  fileloc<-file.path(tempdir(), 'file.txt')
  if (file.exists(fileloc))
  {
    unlink(fileloc)
  }
  m<-testf15(tmpdir)
  depwalker::load.object(metadata=m)
  expect_equal(dir, getwd())
  expect_true(file.exists(fileloc))
})

