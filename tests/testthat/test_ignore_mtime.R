context("Execution of simple task")

source('testfunctions.R')
#source('tests/testthat/testfunctions.R')

test_that("Try ignoring mtime (18)", {
  m<-testf18(tmpdir);
  depwalker:::get.object(metadata=m,
                         flag.save.in.background = FALSE)

  side_effect_file<-pathcat::path.cat(tmpdir, 'test18_side_effect.txt')
  unlink(side_effect_file)
  Sys.setFileTime(pathcat::path.cat(tmpdir, 'a.rds'), as.POSIXct.Date(as.Date('1979-01-20')))
  depwalker:::get.object(metadata=m,
                         flag.save.in.background = FALSE)
  testthat::expect_true(file.exists(side_effect_file))
  unlink(side_effect_file)
  Sys.setFileTime(pathcat::path.cat(tmpdir, 'a.rds'), as.POSIXct.Date(as.Date('1979-01-20')))
  depwalker:::get.object(metadata=m,
                         flag.save.in.background = FALSE, flag.ignore.mtime = TRUE)

  testthat::expect_false(file.exists(side_effect_file))

})

