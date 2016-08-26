context("Execution of simple task")

test_that("Run simple task", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  if (exists('x',envir=.GlobalEnv))
    rm('x',envir=.GlobalEnv)
  depwalker:::get.object(metadata=m,
                         metadata.path=file.path(tmpdir, "task1"),
                         objectname="x",
                         flag.save.in.background = FALSE)
},1:10))

test_that("Re-run simple task from memory", expect_equal({
  testf1(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task1"));
  depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task1"));
  depwalker:::get.object(metadata=m,
                         metadata.path=file.path(tmpdir, "task1"),
                         objectname="x",
                         flag.save.in.background = FALSE)
},1:10))

test_that("Re-run simple task from disk", expect_equal({
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

test_that("Execute simple task with multiple outputs", expect_equal({
  testf5(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task5"));
  if (exists('a2',envir=.GlobalEnv))
    rm('a2',envir=.GlobalEnv)
  depwalker:::get.object(metadata = m,
                         metadata.path = file.path(tmpdir, "task5"),
                         objectname = "a2",
                         flag.save.in.background = FALSE)
}, 23))
