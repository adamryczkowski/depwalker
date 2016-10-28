context("Data.table specific tests")

source('testfunctions.R')

test_that("definition of task, which outputs data.table", {
  m<-testf6(tmpdir);
  m2<-depwalker:::load.metadata(file.path(tmpdir, "task6"));
  expect_true(depwalker:::are.two.metadatas.equal(m,m2))
})

test_that("Execution of task, which outputs data.table", expect_true({
  testf6(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task6"));
  if (exists('DT',envir=.GlobalEnv))
    rm('DT',envir=.GlobalEnv)
  !is.null(depwalker:::load.object(metadata=m, metadata.path=file.path(tmpdir, "task6"),flag.save.in.background = FALSE))
}))
