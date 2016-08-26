#' Waits for .lock file to disappear
#' @param path Path to the lock file
#' @param name Name of the lock file. Both with \code{path} are used to get
#'   the mangled connection name
#' @param timeout Timeout counted from file's creation time,
#'  after which the lock will be removed automatically.
wait.for.save<-function(path, name, timeout=30*60)
{
  conname<-mangle.connection.name(path,name)

  if(exists(conname))
  {
    con<-eval(parse(text=conname))
    parallel::mccollect(con)
  }
  wait.for.lock(path, timeout)
}


#' Saves the object to disk.
#'
#' It saves the object as a \code{.rds} file using internal R format.
#'
#' For space efficiency it compresses the object with method specified by \code{$compress} method.
#'
#' To speedup rather slow \code{xz} compression, the program tries to take advantage of \code{pxz} utility
#' (Parallel PXZ) if it is available.
#'
#' It also updates task's metadata file with numerous computation statistics and file attributes.
#'
#' @param metadata Metadata that describes the task for which the object is being saved
#' @param objectrecord Object record that describes the object being saved.
#' @param metadata.path Unused
#' @param flag.check.md5sum If set, MD5 hash of the saved file will be calculated and included in the
#'   metadata. Default \code{TRUE}
#' @param flag.forget.object If set, the function will remove the object from memory and perform garbage
#'    collection as soon, as possible.
#' @return 'OK' if success, or string that describe an error if failed.
save.object<-function(
  metadata,
  objectrecord,
  flag.check.md5sum=TRUE,
  flag.forget.object=FALSE)
{
  assertMetadata(metadata)
  assertObjectRecordMetadata(objectrecord, metadata)
  checkmate::assertFlag(flag.check.md5sum)
  checkmate::assertFlag(flag.forget.object)

  filename<-paste0(get.objectpath(objectrecord = objectrecord, metadata = metadata) , getOption('object.save.extension'))
  if (!file.exists(dirname(filename)))
    dir.create(dirname(filename),recursive = TRUE)
  create.lock.file(get.objectpath(metadata = metadata, objectrecord =  objectrecord))

  obj<-eval(parse(text=objectrecord$name),envir=.GlobalEnv)
  objectrecord$objectdigest<-calculate.object.digest(objectrecord$name)
  objectrecord$size<-bit64::as.integer64(as.numeric(object.size(obj)))
  objectrecord$mtime<-NULL
  objectrecord$filesize<-NULL
  objectrecord$filedigest<-NULL
  metadata<-save.metadata(metadata=metadata) #Najpierw zapisujemy metadane, choćby częściowe. O tym, że obiekt jeszcze nie jest dostępny, klienci odczytają na podstawie istniejącego pliku .lock
  filename<-paste0(get.objectpath(objectrecord = objectrecord, metadata = metadata), getOption('object.save.extension'))

  # assign('.tmp',metadata.path,envir=.GlobalEnv)
  # eval(parse(text=paste0('setattr(',metadata$name, ', "digest.metadata", .tmp)')),envir=.GlobalEnv)
  # rm('.tmp',envir=.GlobalEnv)


  if (objectrecord$compress=='xz')
  {
    if (suppressWarnings(length(system('which pxz', intern=TRUE))>0))
    {
      #This trick with parallelizm trades 65% speedup of total execution time into 50% more total combined CPU time
      saveRDS(obj,file=filename,compress=FALSE)
      if (flag.forget.object)
      {
        if (exists(objectrecord$name, envir = .GlobalEnv))
          rm(list=objectrecord$name, envir = .GlobalEnv)
        gc()
      }
      system(paste0('/usr/bin/pxz "', filename, '" -c -T 8 >"', filename, getOption('object.save.extension'), '" && mv -f "', filename, getOption('object.save.extension'), '" "', filename,'"'), wait=TRUE)
    } else
    {
      saveRDS(get(objectrecord$name, envir=.GlobalEnv),file=filename,compress=objectrecord$compress)
      if (flag.forget.object)
      {
        if (exists(objectrecord$name, envir = .GlobalEnv))
          rm(objectrecord$name, envir = .GlobalEnv)
        gc()
      }
    }
  } else
  {
    saveRDS(get(objectrecord$name, envir=.GlobalEnv),file=filename,compress=objectrecord$compress)
    release.lock.file(path=get.objectpath(metadata = metadata, objectrecord =  objectrecord))
    if (flag.forget.object)
    {
      if (exists(objectrecord$name, envir = .GlobalEnv))
        rm(objectrecord$name, envir = .GlobalEnv)
      gc()
    }
  }

  release.lock.file(path=get.objectpath(metadata = metadata, objectrecord =  objectrecord))
  objectrecord$mtime<-file.mtime(filename)
  objectrecord$filesize<-bit64::as.integer64(file.info(filename)$size)
  if (flag.check.md5sum)
    objectrecord$filedigest<-as.character(tools::md5sum(filename))
  metadata$objectrecords[[objectrecord$name]]<-objectrecord
  save.metadata(metadata=metadata) #
  return("OK")
}
