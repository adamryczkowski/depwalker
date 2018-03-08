# It creates unique connection name for object, based on its path and name.
mangle.connection.name<-function(path, objectname)
{
  conname<-digest::digest(path,serialize=FALSE)

  conname<-paste0(objectname, '.con.',basename(path),'.',conname)
  return(conname)
}

