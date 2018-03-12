#' Function that creates the object by calling execution of its script.
#'
#' @param metadata Metadata
#' @param objects_to_keep List of objects, which will be needed by later tasks and should not be removed from memory
#' @param objectaliases Alternate names created objects
#' @param metadata.path metadata.path
#' @param flag_save_intermediate_objects If true, all created objects will be cached. Default: \code{TRUE}
#' @param flag_check_md5sum Gets forwarded to \code{load.objects} function
#' @param flag_save_in_background If true, the process of saving the objects gets forked into another
#'   process and done in parallel in background to speed-up computations. Default: \code{TRUE}
#' @param flag_forget_parents If set, all required prerequisite objects will get removed from the memory,
#'   after the script is run. Default: \code{TRUE}
#' @param estimation_only If \code{NULL} the function is run for its side effects. If not null, it makes no side effects
#'   and instead it returns detailed trace of the computations it is going to perform. This information is gathered
#'   by high-level function \code{metadata.dump}
#' @return If \code{is.null(estiation.only)} it returns either updated metadata on success or NULL on failre.
#' Otherwise it returns updated run statistics
create_objects<-function(
    metadata,
    objects_to_keep=NULL,
    objectaliases,
    target_environment=NULL,
    run_environment=NULL,
    flag_save_intermediate_objects=TRUE,
    flag_check_md5sum=TRUE,
    flag_save_in_background=TRUE,
    flag_forget_parents=TRUE,
    estimation_only=NULL)
{
  checkmate::assertEnvironment(run_environment)
  if(is.null(run_environment)) {
    stop("run.environment is mandatory")
  }
  checkmate::assertEnvironment(target_environment)
  if(is.null(target_environment)) {
    stop("target.environment is mandatory")
  }
  if (!is.logical(estimation_only))
  {
    assertTimeEstimation(estimation_only)
    flag_estimation_only=TRUE
  } else
    flag_estimation_only=FALSE

  #  flag.save.in.background=FALSE
  assertMetadata(metadata)
  checkmate::assertFlag(flag_save_intermediate_objects)
  checkmate::assertFlag(flag_check_md5sum)
  checkmate::assertFlag(flag_save_in_background)
  checkmate::assertFlag(flag_forget_parents)
  checkmate::assertCharacter(objects_to_keep)
  for(n in objects_to_keep)
    assertVariableName(n)
  for(n in objectaliases)
    assertVariableName(n)
  if (length(objects_to_keep)!=length(objectaliases))
    stop("length of objects.to.keep doesn't match length of objectaliases.")
  names<-sapply(metadata$objectrecords, function(or) or$name)
  if (length(names)<length(objects_to_keep))
    stop("More objects to keep than there are objects available in metadata")
  if (length(names)>1)
    matches=objects_to_keep %in% names
  else
    matches=(objects_to_keep==names)
  if (!all(matches))
    stop(paste0("Objects ", objects_to_keep[!matches], " are not defined by metadata ", metadata$path))

  #First we load all ancestors
  ans<-load_and_validate_parents(metadata, flag_check_md5sum, estimation_only=estimation_only,
                                 target_environment=run_environment)

  if (estimation_only)
  {
    if (length(ans)==0)
      return(list()) #Nie udało się wczytać parents
    else
      estimation_only<-ans
  } else
  {
    if (ans==FALSE)
      return(NULL)
  }

  #Next we load all runtime objects

  load_runtime_objects(metadata = metadata, run_environment = run_environment, flag_double_check_digest=flag_check_md5sum)

  #Now we execute the script
  ans<-run_script(metadata, names, estimation_only = estimation_only, run_environment = run_environment)
  if (estimation_only)
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
  if (flag_save_intermediate_objects && !is_inmemory(metadata) )
  {
    if (flag_save_in_background)
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
    if (!flag_save_in_background || 'try-error' %in% attr(con,'class', exact = TRUE))
    {
      metadata<-save_objectrecords(metadata=metadata,
                   envir=run_environment,
                   flag_check_md5sum=flag_check_md5sum,
                   flag_save_in_background=flag_save_in_background)
    }
  }

  #Zapominamy teraz o obiektach nam nie potrzebnych
  flag_do_gc<-FALSE
  for(objrec in metadata$objectrecords)
  {
    if (objrec$name %in% objects_to_keep)
    {
      idx<-which(objects_to_keep == objrec$name)
      oa=objectaliases[idx]

      #This obsoletes the following
      assign(oa, value=run_environment[[objrec$name]], envir=target_environment)
    }
  }

  if (!flag_forget_parents && length(metadata$parents)>0)
  {
    for(i in seq(along.with=metadata$parents))
    {
      parent<-metadata$parents[[i]]
      if (is.null(parent$aliasname))
        n<-parent$name
      else
        n<-parent$aliasname
      assign(n, value=run_environment[[n]], envir = target_environment)
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


#function loads all runtime objects and places them into the run.environment
load_runtime_objects<-function(metadata, run_environment, flag_double_check_digest) {
  inputobjects_df<-get_inputobjects_as_df(metadata)
  storagepath<-inputobjects_storage(metadata)
  objectstorage::load_objects(storagepath=storagepath, objectnames =inputobjects_df$name,
                              target_environment = run_environment,
                              flag_double_check_digest = flag_double_check_digest)
}
