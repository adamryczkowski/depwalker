options(warn=2)
source('tests/testthat/testfunctions.R')
system(paste0('nemo ', tmpdir ))
#m<-testf17(tmpdir)
m<-testf7(tmpdir)
m2<-testf8(tmpdir)

m3<-testf19(tmpdir)

#TODO: Add support for more than one object in the parent record

obj<-get.object(metadata=m3)
obj<-get.object(metadata=m2)
obj<-get.object(metadata=m)
