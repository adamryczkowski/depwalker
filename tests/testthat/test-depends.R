context("Execution of task with dependencies")

source('testfunctions.R')

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

test_that("Testing 'unload.objects' (3)", expect_true({
  if (exists('y3',envir=.GlobalEnv)) rm('y3',envir=.GlobalEnv)

  if (exists('y',envir=.GlobalEnv)) rm('y',envir=.GlobalEnv)

  if (exists('x',envir=.GlobalEnv)) rm('x',envir=.GlobalEnv)

  m<-testf3(tmpdir);
  depwalker:::get.object(
    metadata=depwalker:::load.metadata(file.path(tmpdir, "task3")),
    metadata.path=file.path(tmpdir, "task3"),
    objectname="y3");
  depwalker:::unload.objects(m$parents);
  exists('y3',envir=.GlobalEnv) || exists('y',envir=.GlobalEnv) || exists('x',envir=.GlobalEnv)

}))

test_that("Testing for parallel execution of parents (7)", expect_equal({
  if (exists('ans'))
  {
    rm(ans)
  }
  unlink(file.path(tmpdir,'*.rds'))
  unlink(file.path(tmpdir,'pre_task*'))

  m<-testf7(tmpdir);
  t<-system.time(depwalker:::load.object(metadata=m)) #First run creates the statistics and is not parallel

  Sys.sleep(1) #Wait for parallel save

  #Remove all traces of the computation
  if (exists('ans'))
    rm(ans, envir = .GlobalEnv)
  unlink(file.path(tmpdir,'*.rds'))

  #Run again - this time in parallel
  t2<-system.time(ans<-depwalker:::get.object(metadata=m))
  if (t2['elapsed']*2 > t['elapsed'])
  {
    stop("Parallel evaluation didn't happen")
  }
  ans
},111))
