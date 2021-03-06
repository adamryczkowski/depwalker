#' Function that creates the object by calling execution of its script.
#'
#' @param metadata Metadata
#' @param objects.to.keep List of objects, which will be needed by later tasks and should not be removed from memory
#' @param objectaliases Alternate names created objects
#' @param metadata.path metadata.path
#' @param flag.save.intermediate.objects If true, all created objects will be cached. Default: \code{TRUE}
#' @param flag.check.md5sum Gets forwarded to \code{load.objects} function
#' @param flag.save.in.background If true, the process of saving the objects gets forked into another
#'   process and done in parallel in background to speed-up computations. Default: \code{TRUE}
#' @param flag.forget.parents If set, all required prerequisite objects will get removed from the memory,
#'   after the script is run. Default: \code{TRUE}
#' @param estimation.only If \code{NULL} the function is run for its side effects. If not null, it makes no side effects
#'   and instead it returns detailed trace of the computations it is going to perform. This information is gathered
#'   by high-level function \code{metadata.dump}
#' @return If \code{is.null(estiation.only)} it returns either updated metadata on success or NULL on failre.
#' Otherwise it returns updated run statistics
create.objects<-function(
    metadata,
    objects.to.keep=NULL,
    objectaliases,
    metadata.path,
    target.environment=NULL,
    run.environment=NULL,
    flag.save.intermediate.objects=TRUE,
    flag.check.md5sum=TRUE,
    flag.save.in.background=TRUE,
    flag.forget.parents=TRUE,
    flag.ignore.mtime=FALSE,
    estimation.only=NULL)
{

  if(is.null(run.environment)) {
    stop("run.environment is mandatory")
  }
  if(is.null(target.environment)) {
    stop("target.environment is mandatory")
  }
  if (!is.logical(estimation.only))
  {
    assertTimeEstimation(estimation.only)
    flag.estimation.only=TRUE
  } else
    flag.estimation.only=FALSE

  #  flag.save.in.background=FALSE
  assertMetadata(metadata)
  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)
  checkmate::assertFlag(flag.save.intermediate.objects)
  checkmate::assertFlag(flag.check.md5sum)
  checkmate::assertFlag(flag.save.in.background)
  checkmate::assertFlag(flag.forget.parents)
  checkmate::assertCharacter(objects.to.keep)
  for(n in objects.to.keep)
    assertVariableName(n)
  for(n in objectaliases)
    assertVariableName(n)
  if (length(objects.to.keep)!=length(objectaliases))
    stop("length of objects.to.keep doesn't match length of objectaliases.")
  names<-sapply(metadata$objectrecords, function(or) or$name)
  if (length(names)<length(objects.to.keep))
    stop("More objects to keep than there are objects available in metadata")
  if (length(names)>1)
    matches=objects.to.keep %in% names
  else
    matches=(objects.to.keep==names)
  if (!all(matches))
    stop(paste0("Objects ", objects.to.keep[!matches], " are not defined by metadata ", metadata$path))

  #First we load all ancestors
  ans<-load.and.validate.parents(metadata, flag.check.md5sum, estimation.only=estimation.only,
                                 flag.ignore.mtime=flag.ignore.mtime, target.environment=run.environment)

  if (flag.estimation.only)
  {
    if (length(ans)==0)
      return(list()) #Nie udało się wczytać parents
    else
      estimation.only<-ans
  } else
  {
    if (ans==FALSE)
      return(NULL)
  }

  #Now we execute the script
  ans<-run.script(metadata, names,estimation.only=estimation.only, run.environment=run.environment)
  if (flag.estimation.only)
  {
    return(ans)
  } else {
    if (is.null(ans))
    {
      return(NULL)
    }
  }
  metadata<-ans

  #Now it's time to remove all ancestors from memory, if there were ones
  # for(po in metadata$parents)
  # {
  #   if (is.null(po$aliasname))
  #   {
  #     n<-po$name
  #   } else {
  #     n<-po$aliasname
  #   }
  #   rm(list=n, envir = .GlobalEnv)
  # }
  # if (!is.null(metadata$parents))
  #   gc()

  #Zapisujemy wszystkie wyprodukowane obiekty (nie tylko te, o które byliśmy poproszeni)
  #flag.save.in.background=TRUE
  if (flag.save.intermediate.objects)
  {
    if (flag.save.in.background)
    { # nocov start
      con<-tryCatch(
        parallel::mcparallel(
          save.objects(metadata=metadata,
                       objectnames=NULL,
                       envir=run.environment,
                       flag.check.md5sum=flag.check.md5sum,
                       flag.save.in.background=flag.save.in.background),
          detached=TRUE),
        error=function(e){class(e)<-'try-error';e})  # nocov end
      metadata<-load.metadata(metadata$path)
    }
    if (!flag.save.in.background || 'try-error' %in% attr(con,'class'))
    {
      metadata<-save.objects(metadata=metadata,
                   objectnames=NULL,
                   envir=run.environment,
                   flag.check.md5sum=flag.check.md5sum,
                   flag.save.in.background=flag.save.in.background)
    }
  }

  #Zapominamy teraz o obiektach nam nie potrzebnych
  flag.do.gc<-FALSE
  for(objrec in metadata$objectrecords)
  {
    if (objrec$name %in% objects.to.keep)
    {
      idx<-which(objects.to.keep == objrec$name)
      oa=objectaliases[idx]

      #This obsoletes the following
      assign(oa, value=run.environment[[objrec$name]], envir=target.environment)

#      if (objrec$name != oa)
#      {
#        eval(parse(text=paste0(oa, '<-', objrec$name)),envir=.GlobalEnv)
#        if (exists(objrec$name, envir=.GlobalEnv)) # nocov
#          rm(list=objrec$name,envir=.GlobalEnv) # nocov
#      }
#    } else {
#      if (exists(objrec$name, envir=.GlobalEnv))
#        rm(list=objrec$name,envir=.GlobalEnv)
#      flag.do.gc<-TRUE
    }
  }

  if (!flag.forget.parents && length(metadata$parents)>0)
  {
    for(i in seq(along.with=metadata$parents))
    {
      parent<-metadata$parents[[i]]
      if (is.null(parent$aliasname))
        n<-parent$name
      else
        n<-parent$aliasname
      assign(n, value=run.environment[[n]], envir = target.environment)
#      if ((n %in% objects.to.keep))
#      {
#        if (exists(n, envir=.GlobalEnv))
#          rm(list=n,envir=.GlobalEnv)
#        flag.do.gc<-TRUE
#      }
    }
  }

 # if (flag.do.gc)
#    gc()
  return(metadata)
}
