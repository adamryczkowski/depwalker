context("Execution of task with dependencies")
library(testthat)

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')

test_that("Test whether object with dependecies is calculated (2)", expect_equal({
  if (exists('y',envir=.GlobalEnv)) rm('y',envir=.GlobalEnv)
  testf2(tmpdir);
  depwalker:::get.object(
    metadata=depwalker:::load.metadata(file.path(tmpdir, "task2")),
    metadata.path=file.path(tmpdir, "task2"),
    objectname="y")
}, 55))

test_that("Calculation of object with multiple dependencies (3)", expect_equal({
  if (exists('y3',envir=.GlobalEnv)) rm('y3',envir=.GlobalEnv)
  testf3(tmpdir);
  depwalker:::get.object(
    metadata=depwalker:::load.metadata(file.path(tmpdir, "task3")),
    metadata.path=file.path(tmpdir, "task3"),
    objectname="y3")
}, 60.5))

test_that("Calculation of object with forced multiple dependencies (3)", expect_equal({
  if (exists('y3',envir=.GlobalEnv)) rm('y3',envir=.GlobalEnv)

  if (exists('y',envir=.GlobalEnv)) rm('y',envir=.GlobalEnv)

  if (exists('x',envir=.GlobalEnv)) rm('x',envir=.GlobalEnv)

  testf3(tmpdir);
  depwalker:::get.object(
    metadata=depwalker:::load.metadata(file.path(tmpdir, "task3")),
    metadata.path=file.path(tmpdir, "task3"),
    objectname="y3")
}, 60.5))

# test_that("Testing 'unload.objects' (3)", expect_true({
#   envir=new.env()
#
#   m<-testf3(tmpdir);
#   depwalker:::load.object(
#     metadata=depwalker:::load.metadata(file.path(tmpdir, "task3")),
#     metadata.path=file.path(tmpdir, "task3"),
#     objectname="y3", target.environment = envir);
#
#   testthat::expect_true('y3' %in% names(envir))
#   testthat::expect_false('x' %in% names(envir))
#
#   depwalker:::unload.objects(m$parents, envir = envir);
#   testthat::expect_true(exists('y3',envir=envir))
#
# }))

test_that("Testing for parallel execution of parents (7)", expect_equal({
#  unlink(file.path(tmpdir,'*.rds'))
#  unlink(file.path(tmpdir,'pre_task*'))
#  source('testfunctions.R')

  m<-testf7(tmpdir);
  env<-new.env()
  #First run creates the statistics and is not parallel
  t<-system.time(depwalker:::load.object(metadata=m, target.environment = env, flag.save.in.background = FALSE))

  #Remove all traces of the computation
  env<-new.env()
  unlink(file.path(tmpdir,'*.rds'))

  #Run again - this time in parallel
  t2<-system.time(ans<-depwalker:::get.object(metadata=m))
  if (t2['elapsed']*1.8 > t['elapsed'])
  {
    stop("Parallel evaluation didn't happen")
  }
  ans
},111))

source('testfunctions.R')
test_that("Testing for execution of task, where there is more than one object imported from a single task", {
  m3<-testf19(tmpdir)
  obj<-get.object(metadata=m3, flag.save.in.background = FALSE)

  testthat::expect_equal(obj, 2)
})
