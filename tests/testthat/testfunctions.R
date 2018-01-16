tmpdir<-tempfile('depwalker.test.')
dir.create(tmpdir, showWarnings = FALSE);

testf1<-function(tmpdir) {
  code<-"x<-1:10";
  m<-depwalker:::create.metadata(code, file.path(tmpdir,"task1"))
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf2<-function(tmpdir) {
  testf1(tmpdir);
  code<-"y<-sum(x)";
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task2"));
  m<-depwalker:::add.parent(metadata = m,  name = 'x',   parent.path = file.path(tmpdir, "task1") );
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "y"), name="y");
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf3<-function(tmpdir)
{
  testf2(tmpdir);
  code<-"y3<-mean(inp)+y";
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task3"));
  m<-depwalker:::add.parent(metadata = m, name = 'x',  parent.path  = file.path(tmpdir, "task1"),aliasname = 'inp' );
  m<-depwalker:::add.parent(metadata = m, name = 'y',  parent.path = file.path(tmpdir, "task2"));
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "y3"), name="y3");
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf4<-function(tmpdir)
{
  code<-"ans<-mean(a1,a2,y3)";
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task4"));
  m<-depwalker:::add.parent(metadata = m, name = 'x',  parent.path = file.path(tmpdir, "task1"),aliasname = 'a1' );
  m<-depwalker:::add.parent(metadata = m, name = 'y3',  parent.path = file.path(tmpdir, "task3"));
  m<-depwalker:::add.parent(metadata = m, name = 'y',  parent.path = file.path(tmpdir, "task2"), aliasname = 'a2');
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "ans4"), name="ans");
  depwalker:::make.sure.metadata.is.saved(m);m
}


testf5<-function(tmpdir)
{
  code<-paste0(c("a1<-12", "a2<-23", "a3<-34"),collapse="\n");
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task5"));
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "a1"), name="a1");
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "a2"), name="a2");
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "a3"), name="a3");
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf6<-function(tmpdir)
{
  code<-paste0(c("DT<-as.data.table(list(a=1:10))", "DT[,b:=a+2]"),collapse="\n");
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task6"));
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "DT"), name="DT");
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf7<-function(tmpdir)
{
  fncr<-function(name, value, nr)
  {
    m<-depwalker:::create.metadata(paste0('Sys.sleep(1)\n',name,'<-',value), file.path(tmpdir,paste0("pre_task_",nr)))
    m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, paste0("obj",nr)), name=name)
    depwalker:::make.sure.metadata.is.saved(m)
  }
  #Object with many parents
  m<-depwalker:::create.metadata('ans<-a1+a2+a3', file.path(tmpdir, "task7"))
  ma<-fncr('a1',1,1)
  m<-depwalker:::add.parent(metadata=m, name='a1', parent.path=ma$path);
  ma<-fncr('a2',10,2)
  m<-depwalker:::add.parent(metadata=m, name='a2', parent.path=ma$path);
  ma<-fncr('a3',100,3)
  m<-depwalker:::add.parent(metadata=m, name='a3', parent.path=ma$path);
  m<-depwalker:::add.objectrecord(metadata=m, name='ans')
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf8<-function(tmpdir)
{
  #Nested, complicated task
  m<-depwalker:::create.metadata('ans1<-f7+f2\n ans2<-f7-f2', file.path(tmpdir, "task8"))
  m<-depwalker:::add.objectrecord(metadata=m, name='ans1')
  m<-depwalker:::add.objectrecord(metadata=m, name='ans2')

  ma<-testf7(tmpdir)
  m<-depwalker:::add.parent(metadata=m, parent.path=ma$path, aliasname = 'f7');
  ma<-testf2(tmpdir)
  m<-depwalker:::add.parent(metadata=m, parent.path=ma$path, aliasname = 'f2');
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf9<-function(tmpdir)
{
  #Task, that generates an error
  m<-depwalker:::create.metadata('stop("My error!")', file.path(tmpdir, "task9"))
  m<-depwalker:::add.objectrecord(m,"nothing",file.path(tmpdir, "null"));
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf10<-function(tmpdir)
{
  #Task, that generates a warning
  m<-depwalker:::create.metadata('warning("My warning!")\n nothing<-23', file.path(tmpdir, "task10"))
  m<-depwalker:::add.objectrecord(m,"nothing",file.path(tmpdir, "nothing"));
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf11<-function(tmpdir)
{
  #Task, that generates a message
  m<-depwalker:::create.metadata('message("My message!")\n nothing<-23', file.path(tmpdir, "task11"))
  m<-depwalker:::add.objectrecord(m,"nothing",file.path(tmpdir, "nothing"));
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf12<-function(tmpdir)
{
  #Task, that generates an error deep inside
  m<-depwalker:::create.metadata(c('f<-function() { stop("Nested error!")}',
                                   'f2<-function(par=1) {',
                                   '   if(par==2)',
                                   '      f()',
                                   'return(1)}',
                                   'f3<-function(){f2(2)}',
                                   'f3()')
                                   , file.path(tmpdir, "task12"))
  m<-depwalker:::add.objectrecord(m,"nothing",file.path(tmpdir, "nothing"));
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf13<-function(tmpdir)
{
  #Task that creates lots of data
  m<-depwalker:::create.metadata(c('data<-runif(10000)')
                                 , file.path(tmpdir, "task13"))
  m<-depwalker:::add.objectrecord(m,'data');
  depwalker:::make.sure.metadata.is.saved(m)
  m
}

testf14<-function(tmpdir)
{
  #Task that has more than one source file and a side effect
  folder<-tempdir()
  code<-c(paste0("writeLines('123','", file.path(folder,'testf14.tmp'), "')"),
          "source('aux.R', local=TRUE)")
  m<-depwalker:::create.metadata(code, file.path(tmpdir,"task14"))
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));
  m<-depwalker::add_source_file(m, 'aux.R', 'x<-42')
  m<-depwalker::add_source_file(m, 'aux.txt', 'Whatever')

  depwalker:::make.sure.metadata.is.saved(m);m
}

testf15<-function(tmpdir)
{
  #Task that creates a file in a current directory. Used to test for correct custom script directory
  code<-c("writeLines('123','file.txt')", "x<-13")
  m<-depwalker:::create.metadata(code, file.path(tmpdir,"task15"), execution.directory = tempdir())
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));

  depwalker:::make.sure.metadata.is.saved(m);m
}

testf16<-function(tmpdir)
{
  #Task that has more than one source file and a side effect
  folder<-tempdir()
  code<-"x<-digest::digest('../customdep.bin', algo='crc32', file=TRUE)"
  m<-depwalker:::create.metadata(code, file.path(tmpdir,"task16"))
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));
  towrite<-as.raw(rep(0,16384))
  towrite[3673]<-as.raw(42)
  sciezka<-file.path(folder,'customdep.bin')
  writeBin(object=towrite,con=sciezka)

  m<-depwalker::add_source_file(m, filepath = sciezka,flag.binary = TRUE)

  depwalker:::make.sure.metadata.is.saved(m);m
}

testf17<-function(tmpdir)
{
  #Task with existing code
  filename<-pathcat::path.cat(tmpdir, "existing_R_code.R")

  fileConn<-file(filename)
  writeLines(c("a<-rnorm(1000)","b<-mean(a)"), fileConn)
  close(fileConn)

  m<-depwalker:::create.metadata(metadata.path =  file.path(tmpdir,"task17"), source.path = filename)
  m<-depwalker:::add.objectrecord(m,"b",file.path(tmpdir, "b"));
  depwalker:::make.sure.metadata.is.saved(m);m

}

testf18<-function(tmpdir)
{
  #Simple test with a side effect
  filename<-pathcat::path.cat(tmpdir, "existing_R_code.R")
  code<-"fileConn<-file('test18_side_effect.txt');writeLines(c('Hello World'), fileConn);close(fileConn);Sys.sleep(1);a<-1";
  m<-depwalker:::create.metadata(code, file.path(tmpdir,"task18"))
  m<-depwalker:::add.objectrecord(m,"a",file.path(tmpdir, "a"));
  depwalker:::make.sure.metadata.is.saved(m);m
}
