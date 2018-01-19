#' Makes sure, the task's metadata is saved to disk.
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
#' @param flag.save.in.background If set, the actual compression will happen in the forked thread. The task
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
make.sure.metadata.is.saved<-function(metadata, flag.save.in.background=FALSE,
                                      flag_use_tmp_storage = FALSE,
                                      parallel_cpus=NULL, flag_wait=FALSE, flag_check_hash=TRUE)
{
  assertMetadata(metadata)
  metadata.path<-metadata$path
  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)

  if (file.exists(paste0(metadata.path,getOption('metadata.save.extension'))))
    metadata.disk<-load.metadata(metadata.path)
  else
    metadata.disk<-NULL

  if (length(metadata$objectrecords)==0)
  {
    warning("Saving task without any exported R object. Such tasks are pretty useless")
  }

  if(is.null(metadata.disk))
  {
    metadata<-save_metadata(metadata=metadata, flag.save.in.background=flag.save.in.background,
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
        save_metadata(metadata=metadata.new, flag.save.in.background=flag.save.in.background,
                      flag_use_tmp_storage = flag_use_tmp_storage, parallel_cpus = parallel_cpus,
                      flag_wait = flag_wait, flag_check_hash = flag_check_hash)
        return(metadata.new)
      }
      return(metadata.disk)
    } else {
      metadata<-save_metadata(metadata=metadata, flag.save.in.background=flag.save.in.background,
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
    if (!is.null(o$filesize))
      m$objectrecords[[i]]$filesize<-as.character(o$filesize)
    if (!is.null(o$size))
      m$objectrecords[[i]]$size<-as.character(o$size)
    if (!is.null(o$mtime))
      m$objectrecords[[i]]$mtime<-as.character(o$mtime)
  }

  for (i in seq(along.with=m$inputobjects))
  {
    o<-m$inputobjects[[i]]
    if (!is.null(o$filesize))
      m$inputobjects[[i]]$filesize<-as.character(o$filesize)
    if (!is.null(o$size))
      m$inputobjects[[i]]$size<-as.character(o$size)
    if (!is.null(o$mtime))
      m$inputobjects[[i]]$mtime<-as.character(o$mtime)
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
                               parallel_cpus=NULL, flag_wait=FALSE, flag_check_hash=TRUE) {
  if(!'runtime.environment' %in% names(m))
  {
    return(TRUE)#Nothing to do
  }
  if(!'inputobjects' %in% names(m)) {
    return(TRUE)#nothing to do
  }
  if(is.null(parallel_cpus))
  {
    parallel_cpus<-parallel::detectCores()
  }

  if(parallel_cpus==0) {
    flag_wait=TRUE
  }


  objects<-m$runtime.environment

  objdefs<-dplyr::filter(lists_to_df(m$inputobjects, list_columns = c('name', 'objectdigest', 'size')),filter(!ignored))

  if(flag_wait) {
    wait_for<-'save'
  } else {
    wait_for<-'none'
  }

  jobs<-list()
  for(io in m$inputobjects) {
    file<-get.fullpath(m, io$path)
    if(!is.null(io$filesize)) {
      filesize<-file.size(path)
      if(filesize!=io$filesize){
        unlink(path)
      }
    }
    if(!is.null(io$mtime) && !flag_check_hash) {
      mtime<-file.mtime(path)
      if(mtime != io$mtime) {
        unlink(path)
      }
    }
    if(flag_check_hash && !is.null(io$filedigest)) {
      filedigest<-as.character(tools::md5sum(path))
      if(filedigest!=io$filedigest) {
        unlink(path)
      }
    }
    if(!file.exists(path)) {
      if(length(io$name)>1) {

        #Create a container for all small objects
        e<-new.env()

        for(objname in io$name) {
          assign(objname, objects[[objname]], envir=e)
        }
        jobs[[1]]<-depwalker:::save.large.object(obj=as.list(e), objname, file=file, compress=io$compress,
                                                 wait_for = wait_for, flag_use_tmp_storage = flag_use_tmp_storage,
                                                 parallel_cpus = parallel_cpus,
                                                 flag_detach = FALSE)
      } else {
        jobs[[length(jobs)+1]]<-depwalker:::save.large.object(obj=objects[[io$name]], objname,
                                                              file=file, compress=io$compress,
                                                              wait_for = wait_for,
                                                              flag_use_tmp_storage = flag_use_tmp_storage,
                                                              parallel_cpus = parallel_cpus,
                                                              flag_detach = FALSE)
      }
    }
  }

  fn_wait_and_save<-function(m, jobs, parallel_cpus) {
    #Funkcja zdejmująca blokadę i zapisująca metadane
    parallel::mccollect(jobs, wait=TRUE)
    ios<-list()

    update_metadata_obj<-function(m, objmeta) {
      if(!is.null(objmeta$path)) {
        path<-get.fullpath(m, objmeta$path)
        objmeta$filesize<-file.size(path)
        objmeta$mtime<-file.mtime(path)
        if(flag_check_digest) {
          objmeta$filedigest<-as.character(tools::md5sum(path))
        }
      }
      return(objmeta)
    }
    if(parallel_cpus>0) {
      ios<-parallel::mclapply(m$inputobjects, FUN=update_metadata_obj, mc.cores = parallel_cpus)
    } else {
      ios<-lapply(m$inputobjects, FUN=update_metadata_obj)
    }
    ios<-ios[order(names(ios))]
    m$inputobjects<-ios
    depwalker:::save.metadata(m)
    return(m)
  }
  if(flag_wait) {
    m<-fn_wait_and_save(m, list(), parallel_cpus)
    return(m)
  } else {
    job<-parallel::mcparallel(fn_wait_and_save(m, jobs, parallel_cpus))
    return(job)
  }
}
