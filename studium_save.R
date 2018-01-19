#Testujemy, jak zależy rozmiar obiektu od prędkości zapisu i wielkości wynikowego zbioru

test<-function(size, compress) {
  obj<-data.frame(a=sample.int(size=size, n = 10, replace = TRUE))
  out<-tempfile(fileext = '.rds')
#  czas<-system.time({saveRDS(obj, out, compress = compress); readRDS(out);NULL})[[3]]
#  if(czas==0) {
    czas<-median(microbenchmark::microbenchmark(saveRDS(obj, out, compress = compress))$time)/(1000^3)
#  }
  size<-file.size(out)
  unlink(out)
  return(list(czas=czas, size=size, objsize=object.size(obj)))
}

gen_geom_series<-function(n, start, end, steepness=0.7) {
  offset<-(-1/steepness + 2)*start
  s<-offset + exp(seq(from = log(start-offset), by = log((end-offset)/(start-offset))/(n-1), length.out = n))
  return(s)
}

plot(seria)

library(data.table)

prepare_testdb<-function(compress='gzip', n=100) {
  seria<-gen_geom_series(n, 1, 500000, steepness = 0.01)
  out<-data.table(objsize=rep(NA_integer_, length(seria)),
                  filesize=rep(NA_integer_, length(seria)),
                  czas=rep(NA_real_, length(seria)))

  for(i in seq_along(seria)) {
    cat(paste0("."))
    a<-test(seria[[i]], compress)
    set(out, i, 'objsize', a$objsize)
    set(out, i, 'filesize', a$size)
    set(out, i, 'czas', a$czas)
  }
  out
}


outnone<-prepare_testdb(compress=FALSE, n=100)
outxz<-prepare_testdb(compress='xz', n=100)

library(ggplot2)
library(scales)

plotsize<-function(out) {
  ggplot(data = out, mapping = aes(x=objsize, y=filesize/objsize)) + geom_point() +
    coord_trans(x = 'log10',limx = c(min(out$objsize),max(out$objsize))) +
    annotation_logticks(scaled=FALSE) +
    scale_x_continuous(breaks = trans_breaks("log10", function(x) 10^x),
                       labels = trans_format("log10", math_format(10^.x)))
}


plotsize(out) #Od 5kB opłaca się pliki wkładać do oddzielnych kontenerów z punktu widzenia rozmiaru pliku:
plotczas(out) #Od 5kB opłaca się pliki wkładać do oddzielnych kontenerów z punktu widzenia rozmiaru pliku:

plotczas(outnone)
plotsize(outnone)

plotczas(outxz)
plotsize(outxz)

plotczas<-function(out) {
  ggplot(data = out, mapping = aes(x=objsize, y=czas/objsize)) + geom_point() +
    coord_trans(x = 'log10',limx = c(min(out$objsize),max(out$objsize))) +
    annotation_logticks(scaled=FALSE) +
    scale_x_continuous(breaks = trans_breaks("log10", function(x) 10^x),
                       labels = trans_format("log10", math_format(10^.x)))
}

