context("Error handling and logging")

source('testfunctions.R')

test_that("Testing for handling error during code execution", {
  testf9(tmpdir)
  m<-depwalker:::load.metadata(file.path(tmpdir, "task9"));
  depwalker:::load.object(metadata = m)

  path<-file.path(tmpdir,'task9.err.log')

  if (!file.exists(path))
    stop("Error log after exception is missing")

  l<-readLines(path)

  testthat::expect_equal(object = l, expected = c('> stop("My error!")',
                                                  '',
                                                  'Error: My error! on eval(expr, envir, enclos)',
                                                  'Trace:',
                                                  '1: stop("My error!")'))

})

test_that("Testing for handling warning during code execution", {
  testf10(tmpdir)
  m<-depwalker:::load.metadata(file.path(tmpdir, "task10"));
  depwalker::load.object(metadata = m)
  path<-file.path(tmpdir,'task10.log')

  if (!file.exists(path))
    stop("Error log after exception is missing")

  l<-readLines(path)

  testthat::expect_equal(object = l, expected = c('> warning("My warning!")',
                                                  '',
                                                  'Warning: My warning! on eval(expr, envir, enclos)',
                                                  'Trace:',
                                                  '1: warning("My warning!")',
                                                  '>  nothing<-23'))

})

test_that("Testing for handling messages during code execution", {
  testf11(tmpdir)
  m<-depwalker:::load.metadata(file.path(tmpdir, "task11"));
  depwalker::load.object(metadata = m)
  path<-file.path(tmpdir,'task11.log')
  if (!file.exists(path))
    stop("Error log after exception is missing")

  l<-readLines(path)

  testthat::expect_equal(object = l, expected = c('> message("My message!")',
                                                  '',
                                                  'Message: My message!',
                                                  ' on message("My message!")',
                                                  'Trace:',
                                                  '1: message("My message!")',
                                                  '>  nothing<-23'))

})

test_that("Testing for handling nested error with traceback during code execution", {
  testf12(tmpdir)
  m<-depwalker:::load.metadata(file.path(tmpdir, "task12"));
  depwalker::load.object(metadata = m)
  path<-file.path(tmpdir,'task12.err.log')
  if (!file.exists(path))
    stop("Error log after exception is missing")

  l<-readLines(path)

  testthat::expect_equal(object = l, expected = c('> f<-function() { stop("Nested error!")}',
                                                  '> f2<-function(par=1) {',
                                                  '+    if(par==2)',
                                                  '+       f()',
                                                  '+ return(1)}',
                                                  '> f3<-function(){f2(2)}',
                                                  '> f3()',
                                                  '',
                                                  'Error: Nested error! on f()',
                                                  'Trace:',
                                                  '4: f3()',
                                                  '3: f2(2)',
                                                  '2: f()',
                                                  '1: stop("Nested error!")'
                                                  ))

})

