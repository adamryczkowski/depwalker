#' Metadata in tabular form
#'
#' @param metadata.path Path to task's metadata object
#' @param id Arbitrary ID if this metadata, used as a root for all descendants (ancestors).
#' For details see description.
#'
#' @return Returns list of 4 relational tables that fully describe the metadata of the task.
#' It is should be used, when the tool used to communicate with R does not support lists.
#'
#' Records in the main \code{metadata} table are identified by the \code{ID} field, which
#' contains arbitrary number that distinguish different tasks between each other.
#' The ID of the object loaded from the function argument can be set arbitrarily as
#' \code{id} parameter for ease of parsing the tables with external tools.
#' \strong{\code{metadata}}
#' \describe{
#'   \item{\code{id}}{task's unique ID}
#'   \item{\code{flags}}{integer with the following binary flags:
#'      \enumerate{
#'        \item \strong{1} - All files with code exist on hard drive
#'        \item \strong{2} - When set, never execute the task in parallel
#'        \item \strong{4} - Don't use cached value, force recalculation of this task
#'      }}
#'   \item{\code{path}}{full path to the task metada file}
#'   \item{\code{codepath}}{full path to the R source code}
#'   \item{\code{metadatadigest}}{md5 digest of the task's metadata.
#'      The digest is calculated from three components:
#'      \enumerate{
#'        \item digest of parents
#'        \item digest of exported objects (\code{objectrecords})
#'        \item digest of all source code files
#'      }
#'      Digest is being used to check wether two tasks' metadatas describe equivalent objects}
#'   \item{\code{codedigest}}{md5 digest of all source files. It is a component of \code{metadatadigest}}
#'   \item{\code{code}}{R source code of the task.  If the task needs other code to work,
#'    this code needs to be read separately.}
#' }
#' \strong{\code{objectrecords}}
#' \describe{
#'   \item{\code{parentid}}{task ID, that links each row from this table with table \code{metadata}
#'    in 1-to-many relationship}
#'   \item{\code{flags}}{integer with the following binary flags:
#'      \enumerate{
#'        \item \strong{1} - The object is already in R's memory
#'        \item \strong{2} - The cached object exists, but maybe not update.
#'        \item \strong{4} - The cached object exists, and is update and ready for being loaded without re-computation
#'      }}
#'   \item{\code{path}}{full path to the cached object}
#'   \item{\code{name}}{R name of the object}
#'   \item{\code{compress}}{string describing compression level used when saving cache of the object}
#'   The following items exist only if the task was performed:
#'   \item{\code{size}}{size of the R object, when loaded into R's memory in bytes}
#'   \item{\code{objectdigest}}{digest of the R object, when loaded into R's memory}
#'   The following items exist only if the task was cached:
#'   \item{\code{mtime}}{modification time of the cached object or NULL if the cached object doesn't exist }
#'   \item{\code{filedigest}}{md5 digest of the cached file if the file exists. It can be used to check whether actual
#'     cache is consistent with the task's metafile description}
#'   \item{\code{filesize}}{size of the cached file in bytes}
#' }
#' \strong{\code{parents}}
#' \describe{
#'   \item{\code{parentid}}{task's metadata ID, that links each row from this table with table \code{metadata}
#'    in 1-to-many relationship}
#'   \item{\code{metadatadigest}}{md5 digest of the parent task's metadata}
#'   \item{\code{name}}{original name of the imported parent's object}
#'   \item{\code{aliasname}}{if present, it gives R's name of the parent's object that is
#'    being used in the task's R script. This field gives flexibility in naming objects across tasks}
#'   \item{\code{path}}{path to the parent's task's metadata}
#'  }
#' \strong{\code{timecosts}}
#'
#' Each row describe statistics of each recalculation of this task.
#' \describe{
#'   \item{\code{parentid}}{task's metadata ID, that links each row from this table with table \code{metadata}
#'    in 1-to-many relationship}
#'  \item{\code{walltime}}{Wall time of the task calculation}
#'  \item{\code{cputime}}{CPU user time of the task calculation. Can be greater than \code{walltime} for parallel tasks,
#'    and less than \code{walltime} for disk-bound tasks.}
#'  \item{\code{systemtime}}{CPU kernel time of the task calculation. }
#'  \item{\code{cpumodel}}{Name of the CPU on the machine, where the task was calculated.
#'    We don't support machines with heterogenous CPUs}
#'  \item{\code{corecount}}{Number of CPU cores (\emph{virtual cores are not counted)}}
#'  \item{\code{virtualcorecount}}{Number of virtual cores (including Intel's hyperthreading virtual cores)}
#'  \item{\code{busycpus}}{Number of busy virtual cores just before the task was started.
#'    It gives a measure of system load when computing the task.}
#'  \item{\code{membefore}}{Amount of free system memory before task was started}
#'  \item{\code{memafter}}{Amount of free system memory immidiately after calculation and \code{gc()} .
#'    Memory fragmentation, leakage, and resulting object sizes all contribute to this memory}
#' }
#' @export
metadata.dump<-function(metadata.path, id=NULL)
{
  if (is.null(id))
    id<-as.integer(1)
  myid<-id
  m<-load.metadata(metadata.path)
  if (is.null(m))
    stop(paste0("No metadata found in ", metadata.path))
  base<-metadata.one.dump(m, myid)
  for(i in seq(along.with=m$parents))
  {
    myid<-myid+1
    path<-get.parentpath(parentrecord = m$parents[[i]],
                         metadata=m,
                         flag_include_extension=FALSE)
    a<-metadata.dump(path, myid)
    myid<-a$id
    a$id<-NULL
    base<-join.metadata.dumps(base,a)
  }
  if (id!=1)
    base$id<-myid
  return(base)
}

join.metadata.dumps<-function(dump1, dump2)
{
  ans<-list(
    metadata=rbind(dump1$metadata, dump2$metadata),
    objectrecords=rbind(dump1$objectrecords, dump2$objectrecords),
    parents=rbind(dump1$parents, dump2$parents),
    timecosts=rbind(dump1$timecosts, dump2$timecosts)
  )
  return(ans)
}

metadata.one.dump<-function(metadata, id)
{
  m<-metadata
  f1<-file.exists(get.codepath(metadata))
  if (f1)
    flags=1
  else
    flags=0

  if (m$flag.never.execute.parallel)
    flags<-flags+2
  if (m$flag.force.recalculation)
    flags<-flags+4

  m$flag.force.recalculation<-NULL
  m$flag.never.execute.parallel<-NULL
  m$flags<-flags
  m$id<-id

  m$metadatadigest<-metadata.digest(metadata)

  DT<-data.table::data.table(
    name=character(0),
    path=character(0),
    compress=character(0),
    mtime=character(0),
    filedigest=character(0),
    filesize=bit64::integer64(0),
    size=bit64::integer64(0),
    objectdigest=character(0),
    flags=integer(0),parentid=integer(0))


  if (length(metadata$objectrecords)>0)
  {
    for(i in seq(along.with=metadata$objectrecords))
    {
      o<-m$objectrecords[[i]]
      f1<-take.object.from.memory(o, flag.dry.run=TRUE)
      f2<-file.exists(paste0(get.objectpath(objectrecord = o, metadata = metadata), getOption('object.save.extension')))
      if (f2)
        f4<-load.object.from.disk(metadata=metadata, objectrecord = o, flag.dont.load = TRUE )=='OK'
      else
        f4<-FALSE
      o$flags<-f1*1+f2*2+f4*4
      o$parentid<-id
      if (!is.null(o$mtime))
      {
        o$mtime<-as.character(o$mtime)
      }
      o$path=get.objectpath(objectrecord = m$objectrecords[[i]], metadata=metadata)
      if (is.null(o$filesize))
      {
        o$filesize<-bit64::as.integer64(NA)  #Otherwise weird values get inserted due to the bit64 format.
      }
      if (is.null(o$size))
      {
        o$size<-bit64::as.integer64(NA)
      }
      DT<-rbind(DT, o, fill=TRUE)
    }
  }
  o<-DT
  m$objectrecords<-NULL

  dt<-data.table::data.table(
    name=character(0),
    path=character(0),
    aliasname=character(0),
    metadatadigest=character(0),
    parentid=integer(0))
  if (length(metadata$parents)>0)
  {
    for(i in seq(along.with=metadata$parents))
    {
      p<-m$parents[[i]]
      if (is.null(p$aliasname))
        p$aliasname<-NA
      m2<-load.metadata(get.parentpath(
        parentrecord = p,
        metadata = metadata,
        flag_include_extension=FALSE))
      if (!is.null(m2))
      {
        p$metadatadigest<-metadata.digest(m2)
      } else {
        p$metadatadigest<-NA
      }
      p$path=get.parentpath(parentrecord = m$parents[[i]],metadata=metadata)
      p$parentid<-id
      # if (i==1)
      # {
      #   dt<-data.table::as.data.table(p)
      # } else
      # {
        dt<-rbind(dt, p)
      # }
    }
  }
  p<-dt
  m$parents<-NULL
  m$codedigest<-calculate.code.digest(metadata = metadata)
  m$codepath<-get.codepath(metadata = metadata)
  m$code<-paste(m$code, collapse='\n')

  if (length(m$timecosts)>0)
  {
    m$timecosts[, parentid:=id]
    t<-m$timecosts
  } else {
    t<-data.table::data.table(
      walltime=bit64::integer64(0),
      cputime=bit64::integer64(0),
      systemtime=bit64::integer64(0),
      cpumodel=character(0),
      membefore=integer(0),
      memafter=integer(0),
      corecount=integer(0),
      virtualcorecount=integer(0),
      busycpus=integer(0),
      parentid=integer(0))
  }
  m$timecosts<-NULL

  m<-data.table::as.data.table(m)
  return(list(metadata=m, parents=p, objectrecords=o, timecosts=t))
}


#Funkcja zwraca estymowany czas, jaki zajmuje wykonanie skryptu. Algorytm jest skomplikowany:
# # Jeśli typ procesora jest ten sam, co nasz, to zwracana jest mediana ze wszystkich rekordów o naszym typie procesora
# # W przeciwnym razie zwracana jest globalna mediana
# Jeśli nie ma rekordów, to zwracane są NA
script.time<-function(metadata)
{
  #TODO
}
