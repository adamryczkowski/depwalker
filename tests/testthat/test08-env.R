context("Correct hanlding of environments and promisses")

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')


test_that("Testing for promise", {
  m<-testf13(tmpdir)

  env<-new.env(parent =  .GlobalEnv)
  depwalker:::load.object(metadata=m,flag.save.in.background = FALSE, target.environment=env)
  rm('data', envir = env)

#  debugonce(depwalker:::load.object.from.disk)
  depwalker:::load.object(metadata=m,flag.save.in.background = FALSE, target.environment=env)
#  debugonce(depwalker:::load.objects.by.metadata)

  unlink(paste0(tmpdir, '/data.rds'))

  testthat::expect_warning(testthat::expect_error(env$data, regexp='cannot open the connection'), regexp = 'No such file or directory')
})

