
#' Retrieves the object described by the task from memory.
#'
#' It doesn't compute anything - it just makes sure, that the already-present
#' object residing in memory is a direct and unmodified result from the task run.
#'
#' @param objectrecord Object record, that describe this particular object to retrieve
#' @param alasname If set, it copies the retrieved object into new name.
#'    R performs lazy copies with copy-on-write behaviour so unless you modify the original
#'    object, this is memory efficient.
#' @param flag.check.object.digest If set, it calculates MD5 hash of the object and compares it
#'    with the objectrecord's. If mismatch, then no object is returned. Default \code{TRUE}
#' @param flag.dry.run If set, it leaves no side effects, just checks for the availability of the object.
#'
#' @return \code{TRUE} if object is found and \code{FALSE} otherwise. If true, the object can be found
#'    in \code{.GlobalEnv} under the name \code{aliasname} if present, and original name.
#'
take.object.from.memory<-function(objectrecord, aliasname=NULL, target.environment=NULL, flag.check.object.digest=TRUE, flag.dry.run=FALSE)
{
  # Musimy się upewnić, że obiekt w pamięci to ten sam co w parentrecord.
  # Musi spełniać następujące cechy:
  # - nazwa musi się zgadzać
  # - ścieżka zapisana w metadanych obiektu musi się zgadzać
  # - objectsize musi się zgadzać lub
  # - objectdigest musi się zgadzać

  if(is.null(target.environment)) {
    stop("target.environment is an obligatory argument")
  }

  if (exists(objectrecord$name,envir = target.environment))
    obj<-get(x=objectrecord$name, envir = target.environment)
  else
    return(FALSE) #nie ma obiektu lub zła nazwa

  if (!flag.check.object.digest)
  {
    if (!is.null(aliasname) & !flag.dry.run)
      if (aliasname!=objectrecord$name)
        assign(aliasname, obj, envir = target.environment)
    return(TRUE) #Skoro mamy nie sprawdzać object digestu, to znaczy że na tym etapie obiekt jest ok.
  }

  if (is.null(objectrecord$objectdigest))
    return(FALSE) #Nie mamy objectdigestu, więc nie mamy jak sprawdzić, czy obiekt jest OK. Dlatego FALSE

  crc<-calculate.object.digest(objectrecord$name, target.environment)

  if (objectrecord$objectdigest!=crc)
    return(FALSE)

  if (!is.null(aliasname) & !flag.dry.run)
    if (aliasname!=objectrecord$name)
      assign(aliasname, obj, envir = target.environment)
  return(TRUE)
}

#' Loads one or more tasks' objects from disk cache.
#'
#' Object gets loaded only if its file attributes precisely matche those saved in metadata.
#'
#' Full list of file properties  that have to match in order for being loaded:
#'
#' \enumerate{
#'  \item The file must not be locked by another process - i.e. there must
#'    be no file prefixed with .lock present in the directory
#'  \item Objectrecord must have metrics with which we can compare the file properties
#'  \item File size must match
#'  \item Modification time (mtime) must match
#'  \item If \code{flag.check.md5==TRUE}, file MD5 hash must match
#' }
#'
#' @param metadata Task's metadata
#' @param objectrecord Objectrecord item that corresponds to the cached file we want to load
#' @param aliasname If specified, it overrides the name, under which the loaded object will be available to R.
#' @param flag.dont.load If \code{TRUE}, no object ever gets loaded, and \code{aliasname} parameter is ignored.
#'    It allows to check the availability of the cached object instead of loading it
#' @param flag.check.md5 If set, it also checks MD5 hash of the file. For large files it can be relatively long
#'    process.
#' @return Character string that describes the reason, why the object could not be loaded, or
#'   'OK' string to indicate that the object was or can be loaded.
#'
load.object.from.disk<-function(metadata, objectrecord, aliasname=NULL, target.environment=NULL,
                                flag.dont.load=FALSE, flag.check.md5=TRUE,
                                flag.ignore.mtime=FALSE, flag.allow.promises=TRUE)
{
  checkmate::assertFlag(flag.dont.load)
  checkmate::assertFlag(flag.check.md5)
  assertObjectRecordMetadata(objectrecord, metadata)
  if(is.null(aliasname))
    aliasname<-objectrecord$name
  else
    assertVariableName(aliasname)

  if(is.null(target.environment)) {
    stop("target.environment is a mandatory argument")
  }

  #we need to wait until the job that saves that file actually ends
  rds.filename=get.objectpath(objectrecord = objectrecord,metadata = metadata)
  if (!flag.dont.load)
    wait.for.save(path=rds.filename,name=objectrecord$name)

  rds.filename=paste0(rds.filename, getOption('object.save.extension'))

  if (!file.exists(rds.filename))
    return("File doesn't exist") #failed. Path doesn't exist

  if (is.null(objectrecord$mtime))
    return("Inufficient information in objectrecord")

  if (objectrecord$filesize != bit64::as.integer64(file.info(rds.filename)$size))
    return('File size mismatch')

  if(!flag.ignore.mtime) {
    if (abs(as.POSIXct(objectrecord$mtime) - file.mtime(rds.filename))>1)
      return('MTime mismatch')
  }

  #Then we check, if the digest is valid
  if (flag.check.md5 && !is.null(objectrecord$filedigest) )
  {
    md5sum<-tools::md5sum(rds.filename)
    if (md5sum != objectrecord$filedigest)
    {
      return('File digest mismatch')
    }
    #digest is valid, we can load the object itself
    if(!flag.dont.load) {
      if(flag.allow.promises) {
        env<-new.env()
        env$rds.filename<-rds.filename
        delayedAssign(aliasname, value = readRDS(rds.filename), assign.env = target.environment, eval.env = env)
      } else {
        assign(aliasname, value = readRDS(rds.filename), envir = target.environment)
      }
    }
    #              setattr(obj,'digest.metadata',parentrecord)
    #              assign(parentrecord$name,obj,envir=.GlobalEnv)
    return('OK') #OK
  } else {
    if(!flag.dont.load) {
      if(flag.allow.promises) {
        env<-new.env()
        env$rds.filename<-rds.filename
        delayedAssign(aliasname, value = readRDS(rds.filename), assign.env = target.environment, eval.env = env)
      } else {
        assign(aliasname, value = readRDS(rds.filename), envir = target.environment)
      }
    }
    #digest is valid, we can load the object itself
    return('OK') #OK
  }
  stop("It shouldn't be possible to reach this place") # nocov
}

#' Sets the objects to NULL, effectively unloading them.
#' @param parentrecords Parentrecords that describe objects to unload.
unload.objects<-function(parentrecords, envir=NULL)
{
  if(is.null(envir)) {
    browser()
    stop("envir cannot be empty")
  }
  for (parentrecord in parentrecords)
  {
    if(parentrecord$name %in% names(envir)) {
      rm(parentrecord$name, envir = envir)
    }
  }
}


#' Recursively loads and validates all parent records for the given metadata
#'
#' It is a relatively high-level function that participates in recursive loading of the
#' tasks. It is called to make sure, that the immidiate parents are valid and loaded into
#' memory under the correct aliasnames.
#'
#' The function operates in two modes: one with \code{estimation.only=NULL}, where the function
#' is run for its side-effects which result in loading all the necessary prerequisite objects
#' for the task. Otherwise, if \code{estimation.only!=NULL}, the function makes no side effects, but instead
#' it returns detailed and recursive trace of all tasks, that need to be calculated in order
#' to satisfy the dependencies.
#'
#' @param metadata Metadata for which we want to load all parents (dependencies)
#' @param flag.check.md5 This parameter gets forwarded to \code{load.object.from.disk}
#' @param estimation.only If not FALSE, the function performs a dry run, with no parents get loaded,
#'    but extra information gets returned.
#'
#' @return If \code{estimation.only=NULL}, it returns boolean in which \code{TRUE} means success
#'   and \code{FALSE} - failure.
#'
#'   If \code{estimation.only!=NULL} it returns list used by \code{metadata_dump}
#'
load.and.validate.parents<-function(metadata, target.environment=target.environment,
                                    flag.check.md5=FALSE,estimation.only=NULL,
                                    flag.ignore.mtime=FALSE)  #FALSE jeśli się nie uda
{
  checkmate::assertFlag(flag.check.md5)
  assertMetadata(metadata)
  #Funkcja upewnia się, że przodkowie metadata są aktualni i wczytani do naszej sesji.
  #Decyzja o tym, aby danego rodzica wczytać równolegle zależy od tego, czy dany rodzic jest
  #memory bound, czy cpu bound. Oczywiście równoleglanie ma sens tylko dla cpu bound.
  #Po za tym, jeden rodzic (ten największy) powinien być uruchomiony w naszym wątku

  if (length(metadata$parents)==0)
  {
    if(is.logical(estimation.only))
      return(TRUE)
    else
      return(estimation.only)
  }

  parents.sorted<-sort.parentrecords(metadata = metadata)
  parents.objects<-list()

  for(po in parents.sorted)
  {
    m<-load.metadata(pathcat::path.cat(dirname(metadata$path), po$path))
    o<-get.objectrecords(m, po$names)
    parents.objects[[po$path]]<-list(metadata=m, objrec=o, names=po$names, aliasnames=po$aliasnames, metadata.path=po$path)
  }
  if (!is.logical(estimation.only))
  {
    for(po in parents.objects)
    {
      objrec=po$objrec
      estimation.only$parents[po$path]<-load.objects.by.metadata(metadata=po$metadata, metadata.path=po$metadata.path,  objectnames = po$names, aliasnames=po$aliasnames, flag.estimate.only = TRUE, flag.ignore.mtime=flag.ignore.mtime )
    }
    return(estimation.only)
  }

  #Teraz wybieramy największy z obiektów do wczytania w naszym wątku

  fn.objsizes<-function (l)
  {
    sapply(l$objrec, function(objrec) {if(is.null(objrec$size)) {NA;} else {objrec$size;}} )
  }

  o<-order(sapply(parents.objects, fn.objsizes), na.last=TRUE)
  if (length(o)>0)
    first<-o[[1]]
  else
    first<-1 #Nie ma żadnego największego. Dlatego bierzemy pierwszy z brzegu

  memfree<-memfree()
  is.it.worth.to.parallel<-function(l)
  {
    objrecs=l$objrec
    metadata=l$metadata
    if (!is.null(metadata$flag.never.execute.parallel))
    {
      if (metadata$flag.never.execute.parallel)
        return(FALSE)
    }
    os<-metadata.objects.size(metadata)
    load_speed<-getOption('object.load.speed')
    if(is.na(os))
      return(FALSE)
    if (os>memfree/2.5)
      return(FALSE)
    for(objrec in objrecs)
    {
      if (take.object.from.memory(objectrecord = objrec, target.environment = target.environment,
                                  flag.dry.run = FALSE)) {
        return(FALSE) #The object is already there, no need to paralellize
      }
      if (load.object.from.disk(metadata = metadata,objectrecord = objrec,
                                target.environment = target.environment,
                                flag.dont.load = TRUE,
                                flag.check.md5=flag.check.md5,
                                flag.allow.promises = TRUE,
                                flag.ignore.mtime = flag.ignore.mtime)=='OK')
        return(FALSE) #We don't parallelize simple reading from disk
    }
    # load.time<-metadata$filesize * object.load.speed
    # if (load.time > metadata$timecost[3] *1.3  ) #Czas wczytywania ma być przynajmniej 2xszybszy niż czas tworzenia
    #   return(FALSE)
    return(TRUE)
  }

#  browser()

  if (length(parents.objects)>1)
  {
    f<-sapply(parents.objects, is.it.worth.to.parallel)
    f[[first]]<-FALSE #There is no point in parallelizing one object.
    #Let this non-forked object be the biggest one

    #If there are any children eligible for parallel computing, launch them now
    idxs<-which(f) #Indices of parallel tasks
    contexts<-list()
    if (sum(f)>0)
    {
      for(i in 1:sum(f))
      {
        l=parents.objects[[idxs[i]]]
        #We don't translate object names into aliases, because the calculated objects
        #will not be used there
        #browser()
        contexts[[i]]<-parallel::mcparallel(get.objects.by.metadata(metadata=l$metadata,
                                                                    metadata.path=l$metadata.path,
                                                                    objectnames=l$names,
                                                                    flag.drop.list.if.one.object=FALSE,
                                                                    flag.ignore.mtime=flag.ignore.mtime))
      }
    }
  } else
    f<-FALSE


  #Now we launch non-parallel children sequentially
  idxs<-which(!f) #Indices of non-parallel tasks
  for(i in 1:sum(!f))
  {
    l=parents.objects[[idxs[i]]]
    ans<-load.objects.by.metadata(metadata=l$metadata,
                                  metadata.path=pathcat::path.cat(dirname(metadata$path), l$metadata.path),
                                  target.environment=target.environment,
                                  objectnames=l$names, aliasnames=l$aliasnames)
    if (is.logical(ans))
      return (FALSE) #We didn't manage to read
  }

  #Non-parallel tasks are done. Now we make sure all non-parallel are ready as well and we apply aliases
  if (sum(f)>0)
  {
    idxs<-which(f)
    for(i in 1:sum(f))
    {
      l=parents.objects[[idxs[i]]]
      obj<-parallel::mccollect(contexts[[i]])[[1]]
      if (is.null(obj))
        return (FALSE) #Nie udało się wczytać
      for (i in seq(along.with=l$names))
      {
        aliasname<-l$aliasnames[i]
        name<-l$names[i]
        if (!is.null(obj[[name]]))
          assign(aliasname,obj[[name]],envir=target.environment)
        else
          stop(paste0("Variable ",name, " not found in ", l$metadata.path, " after parallel run"))
      }
      rm(obj)
    }
  }
  return(TRUE)
}
