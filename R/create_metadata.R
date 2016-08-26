#' Creates task's metadata object.
#'
#' The function will create the object in memory, it will not be saved to disk.
#' @param code character vector with code. Each element of the vector will be treated as separate line.
#' @param metadata.path path to the task's metadata. The metadata will not be saved with this command, but
#'    the path information is nevertheless required, as it is a mandatory part of the task's metadata specification.
#' @param flag.never.execute.parallel if set, the task will not be executed in parallel. Defaults to \code{FALSE}.
#' @return task's metadata object. One might want to complete object's creation with complementary functions
#'    \code{add.parent} and \code{add.object.record}
#' @export
#' @seealso \code{\link{add.parent}}, \code{\link{add.objectrecord}}
create.metadata<-function(code, metadata.path, flag.never.execute.parallel=FALSE)
{
  code<-unlist(strsplit(code,'\n')) #Makes sure each line is in separate element
  a<-assertCode(code)
  if (a!='')
    stop(paste0("Invalid code: ", a))
  checkmate::assertFlag(flag.never.execute.parallel)
  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)

  metadata<-list(code=code, path=metadata.path, parents=list(), objectrecords=list(), flag.never.execute.parallel=flag.never.execute.parallel, flag.force.recalculation=FALSE)
  assertMetadata(metadata)
  return(metadata)
}


#' Adds parent (ancestor) record to the existing metadata.
#'
#' You specify parent record when you need another task to be already
#' computed as dependency before starting with the current one.
#'
#' If the parent's record with specified name already exists, it silently overwrites it.
#' @param metadata already created metadata you wish to add ancestor to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param metadata.path either full or relative to metadata path with the
#'   ancestor's metadata file.
#' @param name name of the object's name in the parent task you with to import
#' @param aliasname optional argument. If set it specifies alternate name of the imported
#'   \code{name} object as it is used by the task's R code. This setting allows great
#'   flexibility in naming task's output objects.
#' @return modified \code{metadata} argument that includes specified parent's record.
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.objectrecord}}
add.parent<-function(metadata, name=NULL, metadata.path, aliasname=NULL)
{
  assertValidPath(metadata.path)
  assertMetadata(metadata)

  if (is.null(name))
  {
    parent<-load.metadata(metadata.path)
    if (is.null(parent))
      stop("Cannot find the parent")

    counts<-sum(plyr::laply(parent$objectrecords, function(or){names=or$name; return(length(names))}))
    if (counts==1)
      name<-paste0(parent$objectrecords[[1]]$name,collapse="; ")
    else
      stop(paste0("Cannot unambiguously infer name of the imported object as there are ",counts, " exported objects in the parent."))
  }


  checkmate::assertString(name)
  if (!is.null(aliasname))
  {
    assertVariableName(aliasname)
    varname=aliasname
  } else
    varname=name
  parents<-metadata$parents
  parentnames<-sapply(
    parents,
    function(x) {
      if (is.null(x$aliasname)) {
        x$name
      } else {
        x$aliasname
  }})

  if (varname %in% parentnames)
  {
    stop(paste0(varname, " is already present in parents of ", metadata$path))
  }
  path=pathcat::make.path.relative(base.path =  dirname(metadata$path), target.path = metadata.path)
  parents[[path]]<-list(name=name, path=path, aliasname=aliasname)
  metadata$parents<-parents

  assertMetadata(metadata)
  return(metadata)
}

#' Register task's output object.
#'
#' If you run tasks not for side effects, but for creating R objects, you need
#' to formally register each of them.
#'
#' Each registered object gets cached to its own, unique file after a script run.
#' It can also be specified as a dependency of another task.
#'
#' After script run, all objects that are not registered will be removed from memory.
#'
#' If the object's record with specified name already exists, it will be silently overwritten.
#' @param metadata already created metadata you wish to add ancestor to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param name name of the object's name
#' @param path location of the cached file with the computed object. The path can
#'   be absolute or relative to the metadata's path.
#' @param compress optional. You can specify the compression method of the resulting object.
#'   When used \code{'xz'} compression, the computer will try to compress it with the
#'   parallel \code{pxz} application, if it is available on the system.
#' @return modified \code{metadata} argument that includes specified object's record.
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.parent}}
#' @export
add.objectrecord<-function(metadata, name, path=NULL, compress='xz')
{
  checkmate::assertString(name)
  if (is.null(path))
  {
    path = file.path(dirname(metadata$path), name)
  }
  assertValidPath(path)

  assertMetadata(metadata)
  checkmate::assertChoice(compress, c('xz','gzip','bzip2',FALSE))
  objectrecords<-metadata$objectrecords
  objectnames<-sapply(objectrecords, function(x)x$name)
  if (name %in% objectnames)
  {
    #Już jest ta nazwa w parentrrecords... Najpierw usuwamy poprzednią
    warning(paste0('object "', name, '" is already present in the exports of the task. Overwriting.'))
    idx<-which(objectnames==name)
    objectrecords[idx]<-NULL
  }
  path=pathcat::make.path.relative( base.path =  dirname(metadata$path), target.path = path)

  objectrecords[[name]]<-list(name=name, path=path, compress=compress)
  metadata$objectrecords<-objectrecords

  assertMetadata(metadata)
  return(metadata)
}

