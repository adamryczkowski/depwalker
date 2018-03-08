#' Makes sure, the task's metadata is saved to disk.
#'
#' It is quite complicated thing. For one, the data in memory might be different than data on disk
#'
#' For \code{make.sure.metadata.is.saved} if the equivalent metadata is already present on disk,
#' it assumes the saved metadata contains more detailed optional
#' information and instead of saving the \code{metadata} parameter, it returns to old one instead.
#' If exists metadata on disk with the same name, but not equivalent (e.g. with different objectrecords or
#' R code) it gets silently overwritten.
#'
#' The \code{save_metadata} saves the runtime objects, and then calls \code{save.metadata} that saves the actual
#' YAML representation of the metadata
#'
#' The file's format is \emph{yaml}, and it is written specified filename but with
#' custom extension given by the option \code{getOption('metadata.save.extension')}.
#'
#' @param metadata metadata object that you want to make sure is saved on disk.
#' @param path Directory where the metadata and its dependant objects should be save to.
#'        Defaults to the current directory. If relative - it will be prefixed with the current directory.
#' @param flag_save_in_background If set, the actual compression will happen in the forked thread. The task
#'        will be locked from execution until the save finishes.
#' @param flag_use_tmp_storage Relevant only if runtime objects are present and user selects 'xz' compression,
#'        and external \code{pxz} program is present.
#'        Use it when saving to slow network connection to conserve resources and time.
#'        If set it will first copy the object to a temporary file on the fast /tmp disk, then compress it into
#'        the target file using \code{pxz} external program.
#' @param parallel_cpus Relevant only if more than one large runtime objects are present. Override number
#'        of threads used to save the objects into separate archive files. Set zero (0) to force serial work.
#' @param flag_wait Relevant only if runtime objects are present.
#'        If set to \code{FALSE} the actual compression and save of the runtime objects in the background,
#'        freeing up the R session. The metadata YAML object will be saved the last.
#' @param flag_check_hash Relevant only if runtime objects are present in metadata and some are already saved to disk.
#'        If set, prefer \code{file size} and \code{MD5 digest} over \code{file size} and \code{mtime} for assessing
#'        whether the file already present in disk still contains the correct contents.
#' @return returns either the \code{metadata} parameter, or if equivalent task's metadata is found on disk,
#'   it returns the saved metadata.
#' @export
#' @seealso \code{\link{save.metadata}} - unconditionally saves task's metadata on disk.
make_sure_metadata_is_saved<-function(metadata, path=NULL, flag_save_in_background=FALSE,
                                      flag_use_tmp_storage = FALSE, on_overwrite='fail',
                                      parallel_cpus=NULL, flag_wait=TRUE, flag_check_hash=TRUE)
{
  assertMetadata(metadata=metadata, flag_ready_to_run=TRUE)
  checkmate::checkChoice(on_overwrite, choices=c('fail','warn','ignore'))
  if(is.null(path)) {
    path<-basename(metadata$path)
  }
  flag_clear_on_dist<-FALSE
  newpath<-paste0(pathcat::path.cat(getwd(), path, metadata$path), getOption('depwalker.metadata_save_extension'))
  if(!is_inmemory(metadata)) {
    if (get_path(metadata, path)!=get_path(metadata, metadata$path)) {
      stop("We still don't support re-writing already saved task. ")
    } else {
      if(file.exists(newpath)) {
        if(is_cached_value_stale(metadata)) {
          if(on_overwrite=='fail') {
            stop(paste0("The task will overwrite previously saved task. "))
          }
          if(on_overwrite=='warn') {
            warning(paste0("The task will overwrite previously saved task. "))
          }
        } else {
          #2. Sprawdź, czy mamy statystyki.
          #      Jeśli nie, to wczytaj metadata z dysku i sprawdź,
          #      czy zmienił się hash. Jeśli się nie zmienił,
          #      to wczytaj statystyki z dysku i doczep do naszych.
          oldmetadata<-load_metadata(newpath)
          metadata$history<-oldmetadata$history
          flag_clear_on_dist<-TRUE
        }
      }
    }
  }

  #1. Jeśli u nas jest ścieżka relatywna i podano path, to użyj parents jako prefiksu dla naszej i
  #   nadpisz ścieżkę do parents (bo mogła się zmienić, jeśli jest relative).
  if(is_inmemory(metadata) && path!='' && length(metadata$parents)>0 ) {
    for(i in seq_along(metadata$parents)) {
      parent<-metadata$parents[[i]]
      if(!pathcat::is_absolute_path(parent$path)) {
        metadata$parents[[i]]<-NULL
        oldshortpath<-parent$path
        oldpath<-get_path(metadata, parent$path)
        newpath<-pathcat::make.path.relative(base.path = path, target.path = oldpath)
        parent$path<-newpath
        metadata[[newpath]]<-parent
        warning(paste0("Rewriting relative path to parent ", oldshortpath, "=>", newpath,". "))
      }
    }
  }

  metadata$path<-pathcat::path.cat(getwd(), path, metadata$path)

  tryCatch({
    lock<-acquire_lock(metadata)


    if(length(metadata$inputfiles)>0) {
      leave_inputfiles<-names(metadata$inputfiles)
    } else {
      leave_inputfiles<-character(0)
    }

    #Deleting all irrelevant parts
    if(flag_clear_on_dist) {
      delete_metadata(newpath, flag_leave_intputobjects = TRUE, leave_inputfiles = leave_inputfiles, flag_dont_lock=TRUE)
    }


    #Saving history logs
    write_history_output_file(metadata)

    #Saving inputobjects
    if(length(metadata$inputobjects)>0) {
      all_objects<-objectstorage::lists_to_df(metadata$inputobjects)
      all_objects<-dplyr::filter(all_objects, ignored==FALSE)
      all_objects<-intersect(all_objects$name, names(metadata$runtime_environment))
      inputobjects_storage<-get_path(metadata, metadata$inputobjects_storage, extension = 'objectstorage')
      if(!file.exists(inputobjects_storage)) {
        objectstorage::create_objectstorage(inputobjects_storage)
      }
      objst_items<-objectstorage::list_runtime_objects(inputobjects_storage)$objectname


      objectstorage::modify_objects(storagepath = inputobjects_storage, obj.environment = metadata$runtime_environment,
                                    objects_to_add = all_objects,
                                    objects_to_remove = setdiff(objst_items, all_objects))
    }

    #Saving inputfiles
    if(length(metadata$inputfiles)>0) {
      for(inputfile in metadata$inputfiles) {
        if('code' %in% names(inputfile)) {
          path<-get_path(metadata, inputfile$path)
          writeLines(text=inputfile$code, con = path)
        }
      }
    }

    last_history<-get_last_history_statistics(metadata)
    if(!is.null(last_history)) {
      if('output' %in% names(last_history)) {
        output<-normalize_text_string(last_history$output)
        check_if_different_file(path='?', contents=output)
        tmpfile<-tempfile()
        writeLines(text = tmpfile)
      }
    }

    save.metadata(metadata)

  },
  finally = {release_lock(metadata)} )
  assertMetadata(metadata)
  return(metadata)
}


#' @describeIn make.sure.metadata.is.saved unconditionally saves the metadata on disk.
#'
save.metadata<-function(metadata)
{
  assertMetadata(metadata)

  m<-metadata
  m$path<-NULL
  m$runtime_environment<-NULL

  #Simple function that saves metadata on disk

  hists<-metadata$history
  for(i in seq_along(hists)) {
    hists[[i]]$output<-NULL
  }
  m$history<-hists
  # dfhist<-
  #   objectstorage::lists_to_df(metadata$history, list_columns = 'output')
  # m$history<-dfhist
  # m$history[,output:=NULL]
  #
  # m$history[,walltime           := as.numeric(m$history$walltime)]
  # m$history[,cputime          := as.numeric(m$history$cputime)]
  # m$history[,systemtime       := as.numeric(m$history$systemtime)]
  # # m$history[,membefore        := as.character(membefore)]
  # # m$history[,memafter         := as.character(memafter)]
  # # m$history[,corecount        := as.character(corecount)]
  # # m$history[,virtualcorecount := as.character(virtualcorecount)]
  # # m$history[,busycpus         := as.character(busycpus)]

  y<-yaml::as.yaml(m, precision=15)
  path<-get_path(metadata, metadata$path, extension='metadata')
  con<-file(path, encoding = 'UTF-8')
  writeLines(y,con)
  close(con)

  return(metadata)
}

save_runtime_objects<-function(m, parallel_cpus=parallel::detectCores(), flag_wait=FALSE, flag_check_hash=TRUE) {
  if(!'runtime.environment' %in% names(m))
  {
    return(NULL)#Nothing to do
  }
  if(!'inputobjects' %in% names(m)) {
    return(NULL)#nothing to do
  }

  if(length(m$inputobjects)==0) {
    return(NULL)
  }
  if(length(m$runtime.environment)==0) {
    return(NULL)
  }

  if(parallel_cpus==0) {
    flag_wait=TRUE
  }


  objects<-m$runtime.environment

  objdefs<-dplyr::filter(objectstorage::lists_to_df(l = m$inputobjects),filter(!ignored))

  if(flag_wait) {
    wait_for<-'save'
  } else {
    wait_for<-'none'
  }

  browser()
  storagepath<-get_inputobjects_storagepath(m)

  if(is.null(m$flag.use.tmp.storage)){
    flag.use.tmp.storage<-FALSE
  } else {
    flag.use.tmp.storage<-TRUE
  }

  objectstorage::set_runtime_objects(storagepath = storagepath, obj.environment = m$runtime.environment,
                                     objectnames = objdefs$name, flag_use_tmp_storage = flag.use.tmp.storage,
                                     compress = objdefs$compress, wait_for = wait_for,
                                     parallel_cpus = parallel_cpus)
}

#' Function called by make_sure_metadata_is_saved to save all the runtime objects stored in m
#' By default all small objects will kept together, and large objects will live in a separate files.
#'
#' @param metadata already created metadata with 'runtime.environment' with all the objects to save.
#' @param flag_use_tmp_storage If set, it will use temp (presumably fast) storage to save the uncompressed version
#'        of objects fast, and only then it will compress it to the target location. Relevant if the target
#'        location is slow (e.g. network share)
#' @param parallel_cpus You can set it to override automatic resolution of parallel cores. If set to 0,
#'        function will run in serial mode, and force flag_wait=TRUE.
#' @param flag_wait If set, the function will wait until the object is serialized, and return a valid
#'        metadata. Otherwise the save will be performed in the background, locking the metadata in the process.
#' @param flag_check_hash When the object is already serialized, with this flag it check not check size and
#'        mtime, but size and hash (md5) of the serialized object.
#' @return Returns TRUE if successfull
#' @export
#' @seealso \code{\link{create_metadata}}, \code{\link{add_parent}}
#
save_metadata<-function(m, flag_use_tmp_storage = FALSE,
                               parallel_cpus=parallel::detectCores(), flag_wait=FALSE) {

  if(parallel_cpus==0 || flag_wait) {
    save_runtime_objects(m = m, parallel_cpus=parallel_cpus, flag_wait=TRUE)
    depwalker:::save.metadata(m)
    return(m)
  } else {
    job<-parallel::mcparallel({
      save_runtime_objects(m = m, parallel_cpus=parallel_cpus, flag_wait=flag_wait)
      depwalker:::save.metadata(m)
    }, silent = TRUE)
    return(job)
  }


}


#' Removes all traces of the metadata from disk
#'
#' It removes the following files:
#'
#' \itemize{
#' \item{inputobjects}
#' \item{stored on disk cached objects}
#' \item{inputfiles specified by relative path}
#' \item{error logs}
#' \item{normal runtime logs}
#' \item{metadata YAML file itself}
#' }
#'
#' Execution is guarded by the metadata's lock.
#'
#' @param metadata The metadata object or path to it
#' @param flag_leave_intputobjects If set, we will not delete inputobjects nor inputfiles. It is usefull when
#'        overwriting one task with another, possibly simmilar.
#' @return NULL
delete_metadata<-function(metadata, flag_leave_intputobjects=FALSE, flag_remove_inputfiles=FALSE,
                          leave_inputfiles=character(0),
                          flag_dont_lock=FALSE) {
  if('character' %in% class(metadata)) {
    metadata<-load_metadata(metadata)
  }


  tryCatch({
    if(!flag_dont_lock) {
      lock<-acquire_lock(metadata)
    }
    #Task removal require removing any linked objects
    #1. inputobjects - always
    #2. inputfiles if the path is relative
    #3. objectrecords - always
    #4. output

    inputobjects_storage<-get_path(metadata, metadata$inputobjects_storage, extension = 'objectstorage')
    if(!flag_leave_intputobjects) {
      #1. inputobjects
      if(file.exists(inputobjects_storage)) {
        objectstorage::remove_all(inputobjects_storage)
      }
    }

    #2. inputfiles if the path is relative
    if(flag_remove_inputfiles) {
      for(i in seq_along(metadata$inputfiles)) {
        inputfile<-metadata$inputfiles[[i]]
        if(!pathcat::is_absolute_path(inputfile) && !inputfile %in% leave_inputfiles ) {
          unlink(inputfile)
        }
      }
    }

    #3. objectrecords - always
    objectrecords_storage<-get_path(metadata, metadata$objectrecords_storage, extension = 'objectstorage')
    if(file.exists(objectrecords_storage)) {
      objectstorage::remove_all(objectrecords_storage)
    }

    #4. output
    path<-get_path(metadata, metadata$path, extension = 'err_output')
    if(file.exists(path)) {
      unlink(path)
    }
    path<-get_path(metadata, metadata$path, extension = 'output')
    if(file.exists(path)) {
      unlink(path)
    }

    #5. Metadata themselves
    path<-get_path(metadata, metadata$path, extension = 'metadata')
    if(file.exists(path)) {
      unlink(path)
    }
  },
  finally = function(e) {
    if(!flag_dont_lock) {
      release_lock(metadata)
    }
  })
  NULL
}
