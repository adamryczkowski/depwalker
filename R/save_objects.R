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
#' @param metadata Metadata that describes the task for which the object is being saved
#' @param objectrecord Object record that describes the object being saved.
#' @param metadata.path Unused
#' @param flag.check.md5sum If set, MD5 hash of the saved file will be calculated and included in the
#'   metadata. Default \code{TRUE}
#' @return updated objectrecord if success, or string that describe an error if failed.
save.object<-function(
  metadata,
  objectrecord,
  envir=.GlobalEnv,
  flag.check.md5sum=TRUE)
{
  assertMetadata(metadata)
  assertObjectRecordMetadata(objectrecord, metadata)
  checkmate::assertFlag(flag.check.md5sum)

  filename<-paste0(get.objectpath(objectrecord = objectrecord, metadata = metadata) , getOption('object.save.extension'))
  if (!file.exists(dirname(filename)))
    dir.create(dirname(filename),recursive = TRUE)
  create.lock.file(get.objectpath(metadata = metadata, objectrecord =  objectrecord))

  obj<-get(objectrecord$name, envir = envir)
  objectrecord$objectdigest<-calculate.object.digest(objectrecord$name, target.environment=envir)
  objectrecord$size<-bit64::as.integer64(as.numeric(object.size(obj)))
  objectrecord$mtime<-NULL
  objectrecord$filesize<-NULL
  objectrecord$filedigest<-NULL
  #  metadata$objectrecords[[objectrecord$name]]<-objectrecord
  #  metadata<-save.metadata(metadata=metadata) #Najpierw zapisujemy metadane, choćby częściowe. O tym, że obiekt jeszcze nie jest dostępny, klienci odczytają na podstawie istniejącego pliku .lock
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
      system(paste0('/usr/bin/pxz "', filename, '" -c -T 8 >"', filename, getOption('object.save.extension'), '" && mv -f "', filename, getOption('object.save.extension'), '" "', filename,'"'), wait=TRUE)
    } else
    {
      saveRDS(get(objectrecord$name, envir=envir),file=filename,compress=objectrecord$compress)
    }
  } else
  {
    saveRDS(get(objectrecord$name, envir=envir),file=filename,compress=objectrecord$compress)
    release.lock.file(path=get.objectpath(metadata = metadata, objectrecord =  objectrecord))
  }

  release.lock.file(path=get.objectpath(metadata = metadata, objectrecord =  objectrecord))
  objectrecord$mtime<-file.mtime(filename)
  objectrecord$filesize<-bit64::as.integer64(file.info(filename)$size)
  if (flag.check.md5sum)
    objectrecord$filedigest<-as.character(tools::md5sum(filename))
  #  metadata$objectrecords[[objectrecord$name]]<-objectrecord
  #  save.metadata(metadata=metadata) #
  return(objectrecord)
}

#' Saves arbitrary large object to disk using saveRDS. If compression is 'xz', then
#' the file is first saved quickly with no compression, and then the background
#' process is spawned that compresses the file in multithreaded fassion using `pxz`,
#' if the program is available.
#'
#'
#' @param obj The object to be saved
#' @param file Path to the file
#' @param compress Compression method with the same meaning as saveRDS. Default is 'xz'.
#' @param wait If set the function exits only after the object is available to read.
#' @param fn_to_run_afters_save Function that will be run after save. The function will get a single argument - path to the
#' newly created file
#' @return parallel job with the backgroud save, if wait is not 'none'
#' @export
save.large.object<-function(obj, file, compress='xz', wait_for=c('save','compress','none'),
                            flag_use_tmp_storage=FALSE, fn_to_run_after_save=NULL,
                            fn_to_run_after_compress=NULL, parallel_cpus=NULL, flag_detach=FALSE) {
  #Stage2 jest wykonywany w tle nawet wtedy, gdy wait=TRUE. Nie będzie wykonany tylko wtedy, gdy compress=FALSE
  if(!wait_for %in% c('save','compress','none')) {
    stop("wait_for must be one of 'save','compress' or 'none'")
  }
  if(is.null(parallel_cpus))
  {
    parallel_cpus<-parallel::detectCores()
  }
  if(parallel_cpus==0) {
    wait_for<-'compress'
  }
  #Funkcja kompresuje plik po szybkim zapisaniu
  save_fn_stage2<-function(obj, file_from, file_to, compress, fn_to_run_after_compress, parallel_cpus, flag_return_job) {
    pxz_wait <- flag_return_job || is.null(fn_to_run_after_save)
    if (compress=='xz')
    {
      which_pxz<-suppressWarnings(system('which pxz', intern=TRUE))
      if (length(which_pxz)>0)
      {

        if(file_from != file_to) {
          system(paste0(
            which_pxz, ' "', file_from, '" -c -T ', parallel_cpus, ' >"', file_to,
            '.tmp" && mv -f "', file_to, '.tmp" "', file_to,'" && rm "', file_from, '"'), wait=pxz_wait)
        } else {
          system(paste0(
            which_pxz, ' "', file_from, '" -c -T ', parallel_cpus, ' >"', file_to,
            '.tmp" && mv -f "', file_to, '.tmp" "', file_to,'"'), wait=pxz_wait)
        }
      } else
      {
        saveRDS(obj,file=file_to,compress=compress)
      }
    } else
    {
      saveRDS(obj,file=file_to,compress=compress)
    }
    if(!is.null(fn_to_run_after_compress)){
      fn_to_run_after_compress(file_to)
    }
  }


  #Funkcja zapisuje plik na szybko, aby jaknajszybciej oddać sterowanie.
  #Funkcja może być użyta tylko wtedy, gdy nie używamy flag_use_tmp_storage i wait=FALSE
  save_fn_stage1<-function(obj, file, fn_to_run_after_save, flag_use_tmp_storage, flag_return_job,
                           stage2fn, compress, fn_to_run_after_compress, parallel_cpus, flag_compress_async) {
    if(flag_use_tmp_storage){
      filetmp=tempfile(fileext = '.rds')
    } else {
      filetmp=file
    }
    saveRDS(obj, filetmp, compress = FALSE)
    if(!is.null(fn_to_run_after_save)){
      fn_to_run_after_save(file)
    }

    if(compress!=FALSE){
      if(flag_compress_async) {
        job<-parallel::mcparallel(
          stage2fn(obj, file_from=filetmp, file_to=file, compress=compress, fn_to_run_after_compress=fn_to_run_after_compress,
                   parallel_cpus=parallel_cpus, flag_return_job=flag_return_job),
          detached=!flag_return_job)
      } else {
        stage2fn(obj, file_from=filetmp, file_to=file, compress=compress, fn_to_run_after_compress=fn_to_run_after_compress,
                 parallel_cpus=parallel_cpus, flag_return_job=flag_return_job)
        job<-NULL
      }
    } else {
      if(!is.null(fn_to_run_after_compress)){
        fn_to_run_after_compress(file)
      }
    }
    return(job)
  }



  if(wait_for=='compress') {
    save_fn_stage1(obj=obj, file=file, fn_to_run_after_save = fn_to_run_after_save,
                   flag_use_tmp_storage = flag_use_tmp_storage, flag_return_job = !flag_detach,
                   stage2fn = save_fn_stage2, fn_to_run_after_compress=fn_to_run_after_compress, compress = compress,
                   parallel_cpus=parallel_cpus, flag_compress_async=FALSE)
    job<-NULL
  } else if (wait_for=='save') {
    job<-save_fn_stage1(obj=obj, file=file, fn_to_run_after_save = fn_to_run_after_save,
                        flag_use_tmp_storage = flag_use_tmp_storage, flag_return_job = !flag_detach,
                        stage2fn = save_fn_stage2, fn_to_run_after_compress=fn_to_run_after_compress, compress = compress,
                        parallel_cpus=parallel_cpus, flag_compress_async=TRUE)
  } else if (wait_for=='none') {
    job<-parallel::mcparallel(
      save_fn_stage1(obj=obj, file=file, fn_to_run_after_save = fn_to_run_after_save,
                     flag_use_tmp_storage = flag_use_tmp_storage, flag_return_job = !flag_detach,
                     stage2fn = save_fn_stage2, fn_to_run_after_compress=fn_to_run_after_compress, compress = compress,
                     parallel_cpus=parallel_cpus, flag_compress_async=FALSE),
      detached = wait_for=='none')
  }
  return(job)
}

#' Saves all given objects to disk.
#'
#' It uses \code{save.object} function for saving, but tries to save all objects in parallel.
#'
#' After save it updates task's metadata on disk
#'
#' @param metadata Metadata that describes the task for which the object is being saved
#' @param objectnames List of objects to save, or NULL and all objects belonging to the task will be saved.
#' @param flag.check.md5sum If set, MD5 hash of the saved file will be calculated and included in the
#'   metadata. Default \code{TRUE}
#' @param flag.save.in.background If \code{TRUE} will try to save all tasks in parallel.
#' @return updated metadata if success, or string that describe an error if failed.
save.objects<-function(
  metadata,
  envir=NULL,
  objectnames=NULL,
  flag.check.md5sum=TRUE,
  flag.save.in.background=TRUE)
{
  if(is.null(envir)) {
    stop("envir is obligatory argument")
  }
  checkmate::assertFlag(flag.save.in.background)
  if (is.null(objectnames)){
    objectrecords<-metadata$objectrecords
  } else {
    objectrecords<-metadata$objectrecords[objectnames]
  }
  if (flag.save.in.background)
  {# nocov start
    newobjectrecords<-tryCatch(
      parallel::mclapply(objectrecords,
                         function(objectrecord)
                         {
                           save.object(metadata=metadata,
                                       objectrecord=objectrecord,
                                       envir=envir,
                                       flag.check.md5sum=flag.check.md5sum)
                         }),
      error=function(e) {class(e)<-'try-error';e})
    # nocov end

  }
  if (!flag.save.in.background || 'try-error' %in% attr(newobjectrecords,'class', exact = TRUE))
  {
    newobjectrecords<-lapply(objectrecords,
                             function(objectrecord)
                             {
                               save.object(metadata=metadata,
                                           objectrecord=objectrecord,
                                           envir=envir,
                                           flag.check.md5sum=flag.check.md5sum)
                             })
  }
  metadata$objectrecords<-newobjectrecords
  save.metadata(metadata)
  return(metadata)
}

