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
  m<-depwalker:::add.parent(m, name = 'x',  file.path(tmpdir, "task1") );
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "y"), name="y");
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf3<-function(tmpdir)
{
  testf2(tmpdir);
  code<-"y3<-mean(inp)+y";
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task3"));
  m<-depwalker:::add.parent(metadata = m, name = 'x',  metadata.path = file.path(tmpdir, "task1"),aliasname = 'inp' );
  m<-depwalker:::add.parent(metadata = m, name = 'y',  metadata.path = file.path(tmpdir, "task2"));
  m<-depwalker:::add.objectrecord(metadata=m, path=file.path(tmpdir, "y3"), name="y3");
  depwalker:::make.sure.metadata.is.saved(m);m
}

testf4<-function(tmpdir)
{
  code<-"ans<-mean(a1,a2,y3)";
  m<-depwalker:::create.metadata(code, file.path(tmpdir, "task4"));
  m<-depwalker:::add.parent(metadata = m, name = 'x',  metadata.path = file.path(tmpdir, "task1"),aliasname = 'a1' );
  m<-depwalker:::add.parent(metadata = m, name = 'y3',  metadata.path = file.path(tmpdir, "task3"));
  m<-depwalker:::add.parent(metadata = m, name = 'y',  metadata.path = file.path(tmpdir, "task2"), aliasname = 'a2');
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
  m<-depwalker:::add.parent(metadata=m, name='a1', metadata.path=ma$path);
  ma<-fncr('a2',10,2)
  m<-depwalker:::add.parent(metadata=m, name='a2', metadata.path=ma$path);
  ma<-fncr('a3',100,3)
  m<-depwalker:::add.parent(metadata=m, name='a3', metadata.path=ma$path);
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
  m<-depwalker:::add.parent(metadata=m, metadata.path=ma$path, aliasname = 'f7');
  ma<-testf2(tmpdir)
  m<-depwalker:::add.parent(metadata=m, metadata.path=ma$path, aliasname = 'f2');
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
  #Task that has more than one source file
  code<-"source('aux.R')"
  m<-depwalker:::create.metadata(code, file.path(tmpdir,"task14"))
  m<-depwalker:::add.objectrecord(m,"x",file.path(tmpdir, "x"));
  m<-depwalker::add_source_file(m, 'aux.R', 'x<-42')
  m<-depwalker::add_source_file(m, 'aux.txt', 'Whatever')

  depwalker:::make.sure.metadata.is.saved(m);m
}
