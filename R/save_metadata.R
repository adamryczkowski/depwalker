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
#'        Defaults to the current directory.
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
make.sure.metadata.is.saved<-function(metadata, path=NULL, flag_save_in_background=FALSE,
                                      flag_use_tmp_storage = FALSE,
                                      parallel_cpus=NULL, flag_wait=TRUE, flag_check_hash=TRUE)
{
  assertMetadata(metadata)

  browser()
  #TODO: Niech zapis metadata oznaczać będzie następujące rzeczy:
  #1. Sprawdź, czy mamy statystyki.
  #      Jeśli nie, to wczytaj metadata z dysku i sprawdź,
  #      czy zmienił się hash. Jeśli się nie zmienił,
  #      to wczytaj statystyki z dysku i doczep do naszych.
  #2. Jeśli u nas jest ścieżka relatywna i podano path, to użyj parents jako prefiksu dla naszej i
  #   nadpisz ścieżkę do parents (bo mogła się zmienić, jeśli jest relative).
  #3. Zapisz wszystkie niezapisane inputfiles
  #4. Zapisz/zaktualizuj obiekty inputobjects
  #5. Jeśli nasza metadata się zmieniła względem starej, to upewnij się, że objectrecords storage
  #   jest puste.

  if(is.null(path)) {
    metadata.path<-get_path(metadata, basename(metadata$path))
  } else {
    metadata.path<-pathcat::path.cat
  }

  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)

  if(!flag_save_in_background) {
    parallel_cpus<-0
  }

  if (file.exists(paste0(metadata.path,getOption('depwalker.metadata_save_extension'))))
    metadata.disk<-load.metadata(metadata.path)
  else
    metadata.disk<-NULL

  if (length(metadata$objectrecords)==0)
  {
    warning("Saving task without any exported R object. Such tasks are pretty useless")
  }

  if(is.null(metadata.disk))
  {
    metadata<-save_metadata(m=metadata,
                            flag_use_tmp_storage = flag_use_tmp_storage, parallel_cpus = parallel_cpus,
                            flag_wait = flag_wait, flag_check_hash = flag_check_hash)
    return(metadata)
  } else
  {
    ans<-are.two.metadatas.equal(m1 = metadata, m2 = metadata.disk)
    if (ans)
    {
      metadata$timecosts <- metadata.disk$timecosts

      metadata.new<-join.metadatas(base_m = metadata.disk, extra_m = metadata)
      if (!is.null(metadata.new))
      {
        save_metadata(m=metadata,
                      flag_use_tmp_storage = flag_use_tmp_storage, parallel_cpus = parallel_cpus,
                      flag_wait = flag_wait, flag_check_hash = flag_check_hash)
        return(metadata.new)
      }
      return(metadata.disk)
    } else {
      metadata<-save_metadata(m=metadata,
                              flag_use_tmp_storage = flag_use_tmp_storage, parallel_cpus = parallel_cpus,
                              flag_wait = flag_wait, flag_check_hash = flag_check_hash)
      return(metadata)
    }
  }
}


#' @describeIn make.sure.metadata.is.saved unconditionally saves the metadata on disk.
#'
save.metadata<-function(metadata)
{
  assertMetadata(metadata)
  if (is.null(metadata$codepath))
    metadata$codepath=paste0(basename(metadata$path),'.R')

  m<-metadata
  m$path<-NULL
  m$code<-NULL

  if (!is.null(metadata$execution.directory))
  {
    m$execution.directory<-metadata$execution.directory
  }

  for (i in seq(along.with=m$objectrecords))
  {
    o<-m$objectrecords[[i]]
    # if (!is.null(o$filesize))
    #   m$objectrecords[[i]]$filesize<-as.character(o$filesize)
    # if (!is.null(o$size))
    #   m$objectrecords[[i]]$size<-as.character(o$size)
    # if (!is.null(o$mtime))
    #   m$objectrecords[[i]]$mtime<-as.character(o$mtime)
  }

  for (i in seq(along.with=m$inputobjects))
  {
    o<-m$inputobjects[[i]]
    # if (!is.null(o$filesize))
    #   m$inputobjects[[i]]$filesize<-as.character(o$filesize)
    # if (!is.null(o$size))
    #   m$inputobjects[[i]]$size<-as.character(o$size)
    # if (!is.null(o$mtime))
    #   m$inputobjects[[i]]$mtime<-as.character(o$mtime)
  }
  m$runtime.environment<-NULL

  #Simple function that saves metadata on disk

  if (!is.null(metadata$timecosts))
  {
    if (nrow(metadata$timecosts)>0)
    {
      m$timecosts[,walltime         := as.character(walltime)]
      m$timecosts[,cputime          := as.character(cputime)]
      m$timecosts[,systemtime       := as.character(systemtime)]
      # m$timecosts[,membefore        := as.character(membefore)]
      # m$timecosts[,memafter         := as.character(memafter)]
      # m$timecosts[,corecount        := as.character(corecount)]
      # m$timecosts[,virtualcorecount := as.character(virtualcorecount)]
      # m$timecosts[,busycpus         := as.character(busycpus)]
    }
  }

  y<-yaml::as.yaml(m, precision=15)
  con<-file(paste0(metadata$path,getOption('metadata.save.extension')),encoding = 'UTF-8')
  writeLines(y,con)
  close(con)
  #  saveRDS(m,file=paste0(metadata.path,metadata.save.extension),compress='xz')
  con<-file(paste0(metadata$path,'.R'),encoding = 'UTF-8')
  writeLines(metadata$code,con)
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

#' Function called by make.sure.metadata.is.saved to save all the runtime objects stored in m
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
#' @seealso \code{\link{create.metadata}}, \code{\link{add.parent}}
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
