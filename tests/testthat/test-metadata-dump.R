context("Correct logging of task execution")

source('testfunctions.R')

test_that("Testing for simple metadata.dump", {
  m<-testf1(tmpdir)
  dump<-depwalker:::metadata.dump(m$path)
})

test_that("Testing for metadata.dump of already executed task", {
  m<-testf1(tmpdir)
  depwalker:::get.object(metadata=m)
  dump<-depwalker:::metadata.dump(m$path)
})

test_that("Testing for metadata.dump of complicated task with parents",{
  m<-testf8(tmpdir)
  obj<-depwalker:::get.object(metadata=m)
  dump<-depwalker:::metadata.dump(m$path)
#  View(dump$objectrecords)
  if (nrow(dump$metadata)!=7)
    stop("Wrong number of rows in dump$metadata!")
  if (nrow(dump$objectrecords)!=8)
    stop("Wrong number of rows in dump$objectrecords!")
  if (nrow(dump$parents)!=6)
    stop("Wrong number of rows in dump$parents!")
  if (nrow(dump$timecosts)<7)
  {
    print(dump$timecosts)
    stop("Wrong number of rows in dump$timecosts!")
  }

})

