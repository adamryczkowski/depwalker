context("Creating task from existing code")

source('testfunctions.R')

test_that("Task from existing code", {

  m <- testf17(tmpdir)
  obj <- depwalker::get.object(metadata=m)

  expect_lt(abs(obj), 1)

})
