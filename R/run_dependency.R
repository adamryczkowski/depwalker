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
load_object<-function(metadata=NULL,
                      objectnames=NULL,
                      target_environment=.GlobalEnv,
                      flag_save_intermediate_objects=TRUE,
                      flag_check_md5sum=TRUE,
                      flag_save_in_background=FALSE,
                      flag_check_object_digest=TRUE,
                      flag_allow_promises=TRUE)
{

  if('character' %in% class(metadata)) {
    metadata<-load_metadata(metadata)
  }

  assertMetadata(metadata)
#  metadata <-make_sure_metadata_is_saved(metadata = metadata)

  if (!is.null(objectnames))
  {
    assertVariableNames(objectnames)
  }

  ans<-load_objects_by_metadata(
    metadata=metadata,
    objectnames=objectnames,
    aliasnames=objectnames,
    target_environment = target_environment,
    flag_save_intermediate_objects=flag_save_intermediate_objects,
    flag_check_md5sum=flag_check_md5sum,
    flag_save_in_background=flag_save_in_background,
    flag_check_object_digest=flag_check_object_digest,
    flag_allow_promises=flag_allow_promises)
  if(is.null(ans)){
    stop("Error during object execution")
  }
  return(ans)
}

#' @export
#' @describeIn load.object Gets the object by its metadata.
#' If objectname is Null, then all objects get returned.
get_object<-function(
    metadata,
    objectname=NULL,
    flag_save_intermediate_objects=TRUE,
    flag_check_md5sum=TRUE,
    flag_save_in_background=TRUE,
    flag_check_object_digest=TRUE,
    flag_return_list=FALSE)
{

  if('character' %in% class(metadata)) {
    metadata<-load_metadata(metadata)
  }

  assertMetadata(metadata)

  if (is.null(objectname))
  {
    objectname=names(metadata$objectrecords)
  }

  env<-new.env()

  assertVariableNames(objectname)
  ans<-load_object(
    metadata=metadata,
    objectnames=objectname,
    target_environment = env,
    flag_save_intermediate_objects=flag_save_intermediate_objects,
    flag_save_in_background=flag_save_in_background,
    flag_check_object_digest=flag_check_object_digest)
  if (!is.null(ans))
  {

    ans<-list()
    for (o in objectname)
    {
      ans[[o]]<-get(o, envir=env)
    }
    if (length(objectname)>1 || flag_return_list)
      return(ans)
    return(ans[[1]])
  }
  else
    return(NULL)
}



#' High level function, that reads objectnames under the names \code{aliasnames}.
#'
#' If \code{flag_estimate_only} is present, it returns the detailed execution tree and historic execution metrics
#'  instead of executing the scripts.
#'
#' The function is there to decide whether to read the cached values instead of doing full task run.
#'
#'
#' @param metadata.path Path to the task's metadata, that describe the task. Alternatively user can supply
#' @param metadata The metadata object itself. It makes sure, that this metadata is saved to disk.
#' @param objectnames List of objects to return. If omited, all objects registered in this metadata will be returned.
#' @param aliasnames List of new names for the created objects.
#' @param flag_save_intermediate_objects If true, all intermediate objects will be saved, to speed-up future retrieval.
#'   Default: \code{TRUE}.
#' @param flag_check_md5sum This flag is forwarded to \code{save.object} function and is in effect only if
#'  \code{flag.save.intermediate.objects==TRUE}. If set, the \code{save.object} would record file's MD5 hash to the
#'  metadata.
#' @param flag_save_in_background If set, objects will be saved in background, leaving the foreground thread
#'  ready for other things. Default: \code{TRUE}. Ignored, if \code{flag.save.intermediate.objects==FALSE}.
#' @param flag_check_object_digest If set, MD5 signature of all created R objects will be computed and
#'  stored in metadata. It will be used to further verify authenticity of the object, when retrieved from memory.
#' @param flag_estimate_only If set, dry run will be performed, during which the
#'   run metrics will be gathered and returned for use of \code{metadata_dump}.
#' @return Returns updated metadata on success, and \code{NULL} on failure.

load_objects_by_metadata<-function(
    metadata=NULL,
    objectnames,
    aliasnames=NULL,
    target_environment=.GlobalEnv,
    flag_save_intermediate_objects=TRUE,
    flag_check_md5sum=TRUE,
    flag_save_in_background=TRUE,
    flag_check_object_digest=TRUE,
    flag_estimate_only=FALSE,
    flag_allow_promises=TRUE) #True oznacza, że się udało
{
  flag_force_recalculation<-metadata$flag_force_recalculation

  # if (!flag.force.recalculation )
  # {
  #   code_changed<-code_has_been_changed(metadata)
  #   if (!is.null(code_changed)){
  #     flag.force.recalculation <- TRUE
  #     metadata<-code_changed
  #   }
  # }
  assertMetadata(metadata)
  if(!is_inmemory(metadata)) {
    metadata<-load_metadata(metadata$path)
  } else {
    stop("Cannot work with not saved metadata")
  }
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
  checkmate::assertFlag(flag_save_intermediate_objects)
  checkmate::assertFlag(flag_check_md5sum)
  checkmate::assertFlag(flag_check_object_digest)

  object_df<-get_objectrecords_as_df(metadata, filter_objectrecords=objectnames)
  object_df$aliasnames<-aliasnames
  if (nrow(object_df)< length(objectnames))
  {
    if(length(objectnames)==1) {
      msg<-paste0("Cannot find object ", objectnames, " in the metadata ", metadata$path)
    } else {
      msg<-paste0("The following objects are not exported from the task, ",
                  metadata$path, ": ", paste0(setdiff(objectnames, object_df$name), collapse=','))
    }
    stop(msg)
  }
  if (nrow(object_df)==0)
  {
    if (flag_estimate_only)
      return(list())
    else
      return(FALSE)
  }



  if (flag_estimate_only)
  {
    ans<-list(objects=objectnames, load_modes=rep(0, times=length(objectnames)))
    names(ans$load_modes)<-objectnames
  } else
  {

    if(is_metadata_locked(metadata)) {
      message(paste0("Waiting to lock ", get_path(metadata=metadata, basename(metadata$path), extension='lock'), "..."))
    }
    acquire_lock(metadata)
    ans<-FALSE
  }
  tryCatch({
    staleness<-is_cached_value_stale(metadata)
    if(is.na(staleness)) {
      staleness<-TRUE
    }

    if(staleness==FALSE && !flag_force_recalculation) {
      # Task is not stale. We can try loading the caches
      object_df$inmem<-FALSE

      # Try load from memory. All successfully loaded objects mark as TRUE in memobjects
      for(i in seq(nrow(object_df)))
      {
        #objectnames<-objectnames[i]
        tryCatch(
          object_df[i, 'inmem']<-take_object_from_memory(
            objectrecord = object_df$name[[i]],
            aliasname = object_df$aliasnames[[i]],
            target_environment=target_environment,
            flag_check_object_digest = flag_check_object_digest,
            flag_dry_run = flag_estimate_only),
          error = function(e) e
        )
      }


      if (flag_estimate_only) {
        ans$load_modes[[memobjects]]<-1
      }

      #Do we still need to load from disk?
      if (all(object_df$inmem))
      {
        if (flag_estimate_only) {
          return(ans)
        } else {
          return(metadata)
        }
      }

      #Now we load all remaining objects from disk
      object_df<-dplyr::filter(object_df, inmem==FALSE)
      object_df$indisk<-FALSE
      if(nrow(object_df)>0) {
        objects<-objectstorage::list_runtime_objects(storagepath=objectrecords_storage(metadata))$objectnames
        objects_to_extract<-intersect(objects, object_df$name)
        if(length(objects_to_extract)>0) {
          pos<-purrr::map_dbl(objects_to_extract, ~which(. %in% object_df$name))
          aliasnames<-object_df$aliasnames[pos]
          browser()
          tryCatch(
            tmp<-objectstorage::load_objects(storagepath=objectrecords_storage(metadata),
                                             objectnames=object_df$name,
                                             aliasnames=object_df$aliasnames,
                                             target_environment=target_environment
            )
          )
          if('error' %in% class(ans)) {
            break
          }
          object_df[i, 'indisk']<-tmp
        }

        # Objects already processed (already loaded into memory) are not needed to be processed anymore:
        if (flag_estimate_only)
        {
          ans$load_modes[diskobjects]<-2
          ans$disk_load_time<-foreach::foreach(i=seq(along.with=objrecs), .combine='+') %do%
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
        if (sum(object_df$inmem)+sum(object_df$indisk)==nrow(object_df))
        {
          if (flag_estimate_only)
            return(ans)
          else
          {
            return(metadata)
          }
        }
      }
    }
    # TODO Następna linia może jest zła. Jak mam zagnieżdżać environments?
    run_environment<-new.env(parent=target_environment)
    create_ans<-create_objects(metadata=metadata,
                               objects_to_keep=objectnames,
                               objectaliases=aliasnames,
                               run_environment=run_environment,
                               target_environment=target_environment,
                               flag_save_intermediate_objects=flag_save_intermediate_objects,
                               flag_check_md5sum=flag_check_md5sum,
                               flag_save_in_background=flag_save_in_background,
                               estimation_only=ans)
#    browser()
    if (is.null(create_ans))
    {
      return(NULL)
    }
    if (flag_force_recalculation)
    {
      create_ans$flag_force_recalculation<-FALSE
      save.metadata(metadata = create_ans)
    }
    return(create_ans)

  },
  finally={
    release_lock(metadata)
  }
  )

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
    flag.ignore.mtime=FALSE,
    flag.drop.list.if.one.object=TRUE)
{
  env<-new.env()
  if (!is.null(load.objects.by.metadata(
          metadata=metadata,
          metadata.path=metadata.path,
          objectnames=objectnames,
          aliasnames = aliasnames,
          target.environment=env,
          flag.save.intermediate.objects=flag.save.intermediate.objects,
          flag.check.md5sum=flag.check.md5sum,
          flag.ignore.mtime = flag.ignore.mtime,
          flag.save.in.background=flag.save.in.background)))
  {
    ans<-list()
    for (n in objectnames)
    {
      ans[[n]]<-get(n, envir=env)
    }
    if (length(objectnames)>1 || !flag.drop.list.if.one.object)
      return(ans)
    return(ans[[1]])
  } else
    return(NULL)
}
