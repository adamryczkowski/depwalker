# It creates unique connection name for object, based on its path and name.
mangle.connection.name<-function(path, objectname)
{
  conname<-digest::digest(path,serialize=FALSE)

  conname<-paste0(objectname, '.con.',basename(path),'.',conname)
  return(conname)
}

# Halts execution until the lock is released, or lock expires.
wait.for.lock<-function(path, timeout=NULL)
{
  if(is.null(timeout)) {
    timeout<-getOption('default.lock.time')
  }
  lockfile<-paste0(path,getOption('lock.extension'))
  if (file.exists(lockfile))
  {
    t<-as.numeric(file.mtime(lockfile))
    while (file.exists(lockfile))
    {
      if (t-as.numeric(Sys.time())>timeout)
      {
        release.lock.file(path)
        break;
      }
      Sys.sleep(1)
    }
  }
}

lock.exists<-function(path, timeout=NULL) {
  lockfile<-paste0(path,getOption('lock.extension'))
  if(is.null(timeout)) {
    timeout<-getOption('default.lock.time')
  }
  if (file.exists(lockfile))
  {
    t<-as.numeric(file.mtime(lockfile))
      if (t-as.numeric(Sys.time())>timeout)
      {
        return(FALSE)
      } else {
        return(TRUE)
      }
    } else {
    return(FALSE)
  }
}

#' Creates lock file on a given path. Lock file is just a 0-length file.
#'
#' The file will have an extension getOption('lock.extension')
#'
#' @param path Location of the lock file
create.lock.file<-function(path, timeout=NULL)
{
  if(is.null(timeout)) {
    timeout<-getOption('default.lock.time')
  }
  wait.for.lock(path, timeout)
  write.table(data.frame(), file=paste0(path,getOption('lock.extension')), col.names=FALSE)
}

# Removes the lock file.
release.lock.file<-function(path)
{
  filename<-paste0(path,getOption('lock.extension'))
  if (file.exists(filename))
    unlink(filename)
}

