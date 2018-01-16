context("Creation of task's metadata")
library(depwalker)

#source('tests/testthat/testfunctions.R')

source('testfunctions.R')

test_that("Test create metadata (1)", expect_equal_to_reference({
    tmpdir<-'/tmp';
    code<-"x<-1:10";
    depwalker:::create.metadata(code, file.path(tmpdir,"task1"))
  }, 'metadata1_pre.rds'))

test_that("Test for digest equivalence of metadata", expect_equal({
    code<-"x<-1:10";
    m<-depwalker:::create.metadata(code, file.path(tmpdir,"task1"));
    depwalker:::metadata.digest(m)
  },
  "560ab6052c5dadfa94d8767097f98958"
))

test_that("Test for adding object record (1)", expect_equal_to_reference({
  tmpdir<-'/tmp';
  testf1(tmpdir);
},"metadata1.rds"))

test_that("Test for adding another object record (1)", expect_warning({
  code<-"x<-1:10";
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task1"));
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));
},regexp='object "x" is already present in the exports of the task. Overwriting.'))

test_that("Save and read simple metadata (1)", {
    m<-readRDS('metadata1.rds');
    depwalker:::make.sure.metadata.is.saved(m);
    m2<-depwalker:::load.metadata(m$path);
    expect_equal(depwalker:::metadata.digest(m),depwalker:::metadata.digest(m2))
})

test_that("Test add parent to metadata (2)", {
  m<-testf2(tmpdir);
  m2<-depwalker:::load.metadata(file.path(tmpdir, "task2"));
  expect_true(depwalker:::are.two.metadatas.equal(m,m2))
})

test_that("Test add parent to metadata with alias (3)", {
  m<-testf3(tmpdir);
  m2<-depwalker:::load.metadata(file.path(tmpdir, "task3"));
  expect_true(depwalker:::are.two.metadatas.equal(m,m2))
})

test_that("Test add extra parents (4)", {
  m<-testf4(tmpdir);
  m2<-depwalker:::load.metadata(file.path(tmpdir, "task4"));
  expect_true(depwalker:::are.two.metadatas.equal(m,m2))
})

test_that("Test add extra parents with conflict (4)", expect_error({
  testf1(tmpdir)
  testf3(tmpdir)
  testf4(tmpdir);
  testf10(tmpdir);
  m<-depwalker:::load.metadata(file.path(tmpdir, "task4"));
  m<-depwalker:::add.parent(metadata = m, name = 'bla',  parent.path = file.path(tmpdir, "task10"), aliasname = 'a2')
}, regexp='^a2 is already present in parents of .*task4$'))

test_that("Test task with multiple outputs (5)", {
  m<-testf5(tmpdir);
  m2<-depwalker:::load.metadata(file.path(tmpdir, "task5"));
  expect_true(depwalker:::are.two.metadatas.equal(m,m2))
})

