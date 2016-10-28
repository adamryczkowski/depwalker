#' Gets the object described by the task
#'
#' This function first tries to grap the object from R memory, if already present.
#'
#' If not, it tries to get the object from saved disk cache.
#'
#' Both functions \code{load.object} load the object into the global environment,
#' \code{get.object} also returns the object(s).
#'
#' If no cache is available, it prepares all prerequisites for the script that runs the task,
#' and then it runs the script itself.
#'
#' It makes sure, that all results are saved to speedup future retrievals.
#'
#' @param metadata.path Path to the task's metadata, that describe the task. Alternatively user can supply
#' @param metadata The metadata object itself. It makes sure, that this metadata is saved to disk.
#' @param objectnames List of objects to return. If omited, all objects registered in this metadata will be returned.
#' @param objectname Object to return.
#' @param flag.save.intermediate.objects If true, all intermediate objects will be saved, to speed-up future retrieval.
#'   Default: \code{TRUE}.
#' @param flag.check.md5sum This flag is forwarded to \code{save.object} function and is in effect only if
#'  \code{flag.save.intermediate.objects==TRUE}. If set, the \code{save.object} would record file's MD5 hash to the
#'  metadata.
#' @param flag.save.in.background If set, objects will be saved in background, leaving the foreground thread
#'  ready for other things. Default: \code{TRUE}. Ignored, if \code{flag.save.intermediate.objects==FALSE}.
#' @param flag.check.object.digest If set, MD5 signature of all created R objects will be computed and
#'  stored in metadata. It will be used to further verify authenticity of the object, when retrieved from memory.
#' @return \code{load.object} returns updated metadata on success, and \code{NULL} on failure.
#'   \code{get.object} returns the object itself, if there is only one object to return,
#'   a named list of objects, if requested to return more than one object, or \code{NULL} on failure.
#' @export
load.object<-function(metadata.path=NULL,
                      metadata=NULL,
                      objectnames=NULL,
                      flag.save.intermediate.objects=TRUE,
                      flag.check.md5sum=TRUE,
                      flag.save.in.background=TRUE, flag.check.object.digest=TRUE)
{

  if (is.null(metadata.path) && is.null(metadata))
  {
    stop("You must provider either metadata or metadata.path argument.")
  }

  if (is.null(metadata))
  {
    metadata<-load.metadata(metadata.path)
  } else {
    metadata <-make.sure.metadata.is.saved(metadata = metadata)
  }

  if (is.null(metadata.path))
  {
    metadata.path<-metadata$path
  } else {
    checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)
  }

  if (is.null(objectnames))
    objectnames=names(metadata$objectrecords)

  if (!is.null(objectnames))
  {
    assertVariableNames(objectnames)
  }

  load.objects.by.metadata(
    metadata=metadata,
    metadata.path=metadata.path,
    objectnames=objectnames,
    aliasnames=objectnames,
    flag.save.intermediate.objects=flag.save.intermediate.objects,
    flag.check.md5sum=flag.check.md5sum,
    flag.save.in.background=flag.save.in.background,
    flag.check.object.digest=flag.check.object.digest)
}

#' @export
#' @describeIn load.object Gets the object by its metadata.
get.object<-function(
    metadata.path=NULL,
    metadata=NULL,
    objectname=NULL,
    flag.save.intermediate.objects=TRUE,
    flag.check.md5sum=TRUE,
    flag.save.in.background=TRUE,
    flag.check.object.digest=TRUE)
{

  if (!is.null(metadata.path))
  {
    checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)
    if (!is.null(metadata))
    {
      assertMetadata(metadata)
      if (metadata$path != metadata.path)
      {
        stop ("Ambiguosly aspecified metadata: metadata.path doesn't point to metadata object")
      }
    } else {
      metadata<-depwalker:::load.metadata(metadata.path)
    }
  } else {
    if (!is.null(metadata))
    {
      assertMetadata(metadata)
      metadata.path<-metadata$path
    } else {
      stop("You must provide either metadata.path or metadata parameter.")
    }
  }

  if (is.null(objectname))
  {
    objectname=names(metadata$objectrecords)
  }
  assertVariableNames(objectname)
  ans<-load.object(
    metadata.path=metadata.path,
    metadata=metadata,
    objectnames=objectname,
    flag.save.intermediate.objects=flag.save.intermediate.objects,
    flag.save.in.background=flag.save.in.background,
    flag.check.object.digest=flag.check.object.digest)
  if (!is.null(ans))
  {
    ans<-list()
    for (o in objectname)
    {
      ans[[o]]<-get(o, envir=.GlobalEnv)
    }
    if (length(objectname)>1)
      return(ans)
    return(ans[[1]])
  }
  else
    return(NULL)
}

#' Hi level function, that reads objectnames under the names \code{aliasnames}.
#'
#' If \code{flag.estimate.only} is present, it returns the detailed execution tree and historic execution metrics
#'  instead of executing the scripts.
#' @param metadata.path Path to the task's metadata, that describe the task. Alternatively user can supply
#' @param metadata The metadata object itself. It makes sure, that this metadata is saved to disk.
#' @param objectnames List of objects to return. If omited, all objects registered in this metadata will be returned.
#' @param aliasnames List of new names for the created objects.
#' @param flag.save.intermediate.objects If true, all intermediate objects will be saved, to speed-up future retrieval.
#'   Default: \code{TRUE}.
#' @param flag.check.md5sum This flag is forwarded to \code{save.object} function and is in effect only if
#'  \code{flag.save.intermediate.objects==TRUE}. If set, the \code{save.object} would record file's MD5 hash to the
#'  metadata.
#' @param flag.save.in.background If set, objects will be saved in background, leaving the foreground thread
#'  ready for other things. Default: \code{TRUE}. Ignored, if \code{flag.save.intermediate.objects==FALSE}.
#' @param flag.check.object.digest If set, MD5 signature of all created R objects will be computed and
#'  stored in metadata. It will be used to further verify authenticity of the object, when retrieved from memory.
#' @param flag.estimate.only If set, dry run will be performed, during which the
#'   run metrics will be gathered and returned for use of \code{metadata_dump}.
#' @return Returns updated metadata on success, and \code{NULL} on failure.

load.objects.by.metadata<-function(
    metadata,
    metadata.path,
    objectnames,
    aliasnames=NULL,
    flag.save.intermediate.objects=TRUE,
    flag.check.md5sum=TRUE,
    flag.save.in.background=TRUE,
    flag.check.object.digest=TRUE,
    flag.estimate.only=FALSE) #True oznacza, że się udało
{
  if (is.null(metadata$flag.force.recalculation))
    flag.force.recalculation<-FALSE
  else
    flag.force.recalculation<-metadata$flag.force.recalculation

  if (!flag.force.recalculation )
  {
    code_changed<-code_has_been_changed(metadata)
    if (identical(code_changed, TRUE))
      flag.force.recalculation <- TRUE
  }

  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)
  assertMetadata(metadata)
  for(n in objectnames)
    assertVariableName(n)
  if (is.null(aliasnames))
  {
    aliasnames<-objectnames
  } else {
    for(n in aliasnames)
      assertVariableName(n)
    if (length(objectnames)!=length(aliasnames))
      stop("Length of objectnames and aliasnames doesn't match")
  }
  checkmate::assertFlag(flag.save.intermediate.objects)
  checkmate::assertFlag(flag.check.md5sum)
  checkmate::assertFlag(flag.check.object.digest)

  objrecs<-get.objectrecords(metadata, objectnames)
  if (length(objrecs)==0)
  {
    if (flag.estimate.only)
      return(list())
    else
      return(FALSE)
  }



  if (flag.estimate.only)
  {
    ans<-list(path=metadata.path, objects=objectnames, load.modes=rep(0, times=length(objectnames)))
    names(ans$load.modes)<-objectnames
  } else
  {
    wait.for.lock(paste0(metadata.path,'.meta'), 120*60) #max 2 hours
    create.lock.file(paste0(metadata.path,'.meta'))
    ans<-FALSE
  }

  if (!flag.force.recalculation)
  {
    memobjects<-rep(FALSE,times=length(objrecs))
    for(i in 1:length(objrecs))
    {
      objrec<-objrecs[[i]]
      aliasname<-aliasnames[i]
      #objectnames<-objectnames[i]
      tryCatch(
        memobjects[i]<-take.object.from.memory(
                          objectrecord = objrec,
                          aliasname = aliasname,
                          flag.check.object.digest = flag.check.object.digest,
                          flag.dry.run = flag.estimate.only),
        error = function(e) release.lock.file(paste0(metadata.path,'.meta'))
      )
    }

    if (flag.estimate.only)
      ans$load.modes[memobjects]<-1


    # Objects already processed (already loaded into memory) are not needed to process anymore:
    #  objrecs<-objrecs[!memobjects]
    #  aliasnames<-aliasnames[!memobjects]
    #  objectnames<-objectnames[!memobjects]

    if (sum(memobjects)==length(objrecs))
    {
      if (flag.estimate.only)
        return(ans)
      else
      {
        release.lock.file(paste0(metadata.path,'.meta'))
        return(metadata)
      }
    }

    diskobjects<-rep(NA, times=length(objrecs))
    for(i in 1:length(objrecs))
    {
      if (memobjects[i])
        diskobjects[i]<-FALSE #Already processed
      objrec<-objrecs[[i]]
      aliasname<-aliasnames[i]
      if (!is.null(objrec$filesize))
      {
        #  For large objects quick to create and slow to load/save we are better off running the script
        load.time<-as.numeric(objrec$filesize) * getOption('object.load.speed')
        if (!is.null(metadata$timecosts))
        {
          if (nrow(metadata$timecosts)==0)
          {
            timecosts=sapply(metadata$timecosts, function(tc) tc$systemtime)
            if (load.time > median(timecosts) *1.3  ) #Czas wczytywania ma być przynajmniej 2xszybszy niż czas tworzenia
            {
              diskobjects[i]<-FALSE
            }
          }
        }
      }
      if (is.na(diskobjects[i]))
      {
        tryCatch(
          diskobjects[i]<-load.object.from.disk(metadata=metadata, objectrecord = objrec,  aliasname = aliasname, flag.dont.load = flag.estimate.only, flag.check.md5 = flag.check.md5sum)=='OK',
          error = function(e) release.lock.file(paste0(metadata.path,'.meta'))
        )
      }
    }

    # Objects already processed (already loaded into memory) are not needed to be processed anymore:
    if (flag.estimate.only)
    {
      ans$load.modes[diskobjects]<-2
      ans$disk.load.time<-foreach::foreach(i=seq(along.with=objrecs), .combine='+') %do%
      {
        if (memobjects[i])
          return(0)
        objrec<-objrecs[[i]]
        aliasname<-aliasnames[i]
        if (!is.null(objrec$filesize))
          return(as.numeric(objrec$filesize) * getOption('object.load.speed'))
        else
          return(NA)
      }
    }


    #  objrecs<-objrecs[!diskobjects]
    #  aliasnames<-aliasnames[!diskobjects]
    #  objectnames<-objectnames[!diskobjects]
    if (sum(memobjects)+sum(diskobjects==TRUE)==length(objrecs))
    {
      if (flag.estimate.only)
        return(ans)
      else
      {
        release.lock.file(paste0(metadata.path,'.meta'))
        return(metadata)
      }
    }
  }
  tryCatch(
    create.ans<-create.objects(metadata=metadata,
                               metadata.path=metadata.path,
                               objects.to.keep=objectnames,
                               objectaliases=aliasnames,
                               flag.save.intermediate.objects=flag.save.intermediate.objects,
                               flag.check.md5sum=flag.check.md5sum,
                               flag.save.in.background=flag.save.in.background,
                               estimation.only=ans),
    finally=release.lock.file(paste0(metadata.path,'.meta')))
  if (is.null(create.ans))
  {
    return(NULL)
  }
  if (flag.force.recalculation)
  {
    create.ans$flag.force.recalculation<-FALSE
    save.metadata(metadata = create.ans)
  }
  return(create.ans)
}

get.objects.by.metadata<-function(
    metadata,
    metadata.path,
    objectnames,
    aliasnames=NULL,
    flag.save.intermediate.objects=TRUE,
    flag.check.md5sum=FALSE,
    flag.save.in.background=TRUE,
    flag.forget.parents=TRUE,
    flag.drop.list.if.one.object=TRUE)
{
  if (!is.null(load.objects.by.metadata(
          metadata=metadata,
          metadata.path=metadata.path,
          objectnames=objectnames,
          aliasnames = aliasnames,
          flag.save.intermediate.objects=flag.save.intermediate.objects,
          flag.check.md5sum=flag.check.md5sum,
          flag.save.in.background=flag.save.in.background)))
  {
    ans<-list()
    for (n in objectnames)
    {
      ans[[n]]<-get(n, envir=.GlobalEnv)
    }
    if (length(objectnames)>1 || !flag.drop.list.if.one.object)
      return(ans)
    return(ans[[1]])
  } else
    return(NULL)
}
