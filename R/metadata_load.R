#' Loads the task's metadata object from disk
#'
#' @param metadata.path Full path to the metadata.
#'
#' @return metadata object.
#'
#' @export
load.metadata<-function(metadata.path)
{
  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)
  if (!file.exists(paste0(metadata.path,getOption('metadata.save.extension'))))
    stop(paste0("Metadata file ",metadata.path," doesn't exists."))

  m<-yaml::yaml.load_file(paste0(metadata.path,getOption('metadata.save.extension')))
  m$path<-metadata.path
  con<-file(get.codepath(m),"r", blocking=FALSE)
  code<-readLines(con)
  close(con)
  m$code<-code
  if (!is.null(m$timecosts))
  {
    m$timecosts<-data.table::as.data.table(m$timecosts)
    m$timecosts[,walltime         := bit64::as.integer64(walltime)]
    m$timecosts[,cputime          := bit64::as.integer64(cputime)]
    m$timecosts[,systemtime       := bit64::as.integer64(systemtime)]
    m$timecosts[,membefore        := as.integer(membefore)]
    m$timecosts[,memafter         := as.integer(memafter)]
    m$timecosts[,corecount        := as.integer(corecount)]
    m$timecosts[,virtualcorecount := as.integer(virtualcorecount)]
    m$timecosts[,busycpus         := as.integer(busycpus)]
  }

  for(i in seq(along.with=m$objectrecords))
  {
    o<-m$objectrecords[[i]]
    if (!is.null(o$size))
      m$objectrecords[[i]]$size<-bit64::as.integer64(o$size)
    if (!is.null(o$filesize))
      m$objectrecords[[i]]$filesize<-bit64::as.integer64(o$filesize)
    if (!is.null(o$mtime))
      m$objectrecords[[i]]$mtime<-as.POSIXct(o$mtime, origin='1970-01-01')
  }

  assertMetadata(m)
  return(m)
}

# Simple wrapper, that reads metadata based on the parentrecord
load.metadata.by.parentrecord<-function(metadata, parentrecord)
{
  load.metadata(parentrecord$path)
}

