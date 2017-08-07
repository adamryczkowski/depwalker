context("Correct dumping of task execution information")

source('testfunctions.R')

test_that("Testing for simple metadata.dump (1)", {
  m<-testf1(tmpdir)
  dump<-depwalker:::metadata.dump(m$path)
})

test_that("Testing for metadata.dump of already executed task (1)", {
  m<-testf1(tmpdir)
  depwalker:::get.object(metadata=m)
  dump<-depwalker:::metadata.dump(m$path)
})

test_that("Testing for metadata.dump of complicated task with parents (8)",{
  m<-testf8(tmpdir)
  obj<-depwalker:::get.object(metadata=m)
  dump<-depwalker:::metadata.dump(m$path)
#  View(dump$objectrecords)
  expect_equal(nrow(dump$metadata), 7)
  expect_equal(nrow(dump$objectrecords),8)
  expect_equal(nrow(dump$parents),6)
  expect_gt(nrow(dump$timecosts),5)

})

