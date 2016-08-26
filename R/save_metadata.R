#' Makes sure, the task's metadata is saved to disk.
#'
#' For \code{make.sure.metadata.is.saved} if the equivalent metadata is already present on disk,
#' it assumes the saved metadata contains more detailed optional
#' information and instead of saving the \code{metadata} parameter, it returns to old one instead.
#' If exists metadata on disk with the same name, but not equivalent (e.g. with different objectrecords or
#' R code) it gets silently overwritten.
#'
#' The \code{save.metadata} function always saves the task's metadata, even if the equivalent file
#' already exists.
#'
#' The file's format is \emph{yaml}, and it is written specified filename but with
#' custom extension given by the option \code{getOption('metadata.save.extension')}.
#'
#' @param metadata metadata object that you want to make sure is saved on disk.
#' @return returns either the \code{metadata} parameter, or if equivalent task's metadata is found on disk,
#'   it returns the saved metadata.
#' @export
#' @seealso \code{\link{save.metadata}} - unconditionally saves task's metadata on disk.
make.sure.metadata.is.saved<-function(metadata)
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
    metadata<-save.metadata(metadata=metadata)
    return(metadata)
  } else
  {
    ans<-are.two.metadatas.equal(m1 = metadata, m2 = metadata.disk)
    if (ans)
    {
      metadata.new<-join.metadatas(base_m = metadata.disk, extra_m = metadata)
      if (!is.null(metadata.new))
      {
        save.metadata(metadata=metadata.new)
        return(metadata.new)
      }
      return(metadata.disk)
    } else {
      metadata<-save.metadata(metadata=metadata)
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

  #Prosta funkcja, która zapisuje metadane na dysk.

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
