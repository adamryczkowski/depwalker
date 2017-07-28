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
create.metadata<-function(code=NULL, metadata.path, flag.never.execute.parallel=FALSE, execution.directory='', source.path=NULL)
{
  if(!is.null(code) && !is.null(source.path)) {
    stop(paste0("You cannot give both code and source.path arguments!"))
  }

  if(is.null(code) && is.null(source.path)) {
    stop(paste0("You must provide either code or source.path argument"))
  }

  base_path <- pathcat::path.cat(getwd(),dirname(metadata.path))

  checkmate::assertPathForOutput(metadata.path, overwrite=TRUE)
  if(!is.null(source.path)) {
    source.path <- pathcat::path.cat(base_path, source.path)
    source.path <- pathcat::make.path.relative(base_path, source.path)
    if(!file.exists(pathcat::path.cat(base_path, source.path))) {
      stop(paste0("File under source.path ", pathcat::path.cat(base_path, source.path), " doesn't exist!"))
    }
    code<-readLines(pathcat::path.cat(base_path, source.path))
  }

  code<-normalize_code_string(code)
  a<-assertCode(code)
  if (a!='')
    stop(paste0("Invalid code: ", a))
  checkmate::assertFlag(flag.never.execute.parallel)


  metadata<-list(code=code, path=metadata.path, parents=list(), objectrecords=list(), flag.never.execute.parallel=flag.never.execute.parallel, flag.force.recalculation=FALSE, execution.directory=execution.directory)
  codeCRC<-calculate_code_digest(metadata)
  metadata$codeCRC<-codeCRC

  if (!is.null(source.path)) {
    metadata<-c(metadata, codepath=source.path)
  }

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
add.parent<-function(metadata=NULL, name=NULL, parent.path=NULL,  parent=NULL, aliasname=NULL)
{
  assertMetadata(metadata)
  if(is.null(parent) && is.null(parent.path)){
    stop("You must provide either parent or parent.path")
  }

  if(!is.null(parent)) {
    assertMetadata(parent)
    parent.path <- parent$path
  }

  if(!is.null(parent.path)) {
    assertValidPath(parent.path)
    parent<-load.metadata(parent.path)
  }

  if(!is.null(parent.path) && !is.null(parent)) {
    if (parent$path != parent.path) {
      stop(paste0("Ambivalent options encountered: parent.path (",parent.path,
                  ") and the parent object, that points to the different directory: ", parent$path))
    }
  }

  if (is.null(name))
  {
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
  path=pathcat::make.path.relative(base.path =  dirname(metadata$path), target.path = parent.path)
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
  path=pathcat::make.path.relative( base.path = dirname(metadata$path), target.path = path)

  objectrecords[[name]]<-list(name=name, path=path, compress=compress)
  metadata$objectrecords<-objectrecords

  assertMetadata(metadata)
  return(metadata)
}


#' Add additional source file to the task.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param filepath Path to the file, if the file is already existing (it can be relative to the task's path)
#' @param code Optional string with the contents of the file. If specified and the file does not already exist, the file will be created
#'        with this contents.
#' @param flag.binary If set, the file will be checksummed in binary mode rather than line-by-line. Checksumming line-by-line has the advantage of
#'        being independent on a way the newline character is encoded into the file. When source is input as source string, the \code{flag.binary}
#'        is automatically set to \code{FALSE}.
#' @param flag.checksum If set (which is default), the file's contents will be examined to check for changes when evaluating freshness of this job.
#' @return modified \code{metadata} argument that includes additional source file or NULL if error.
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.parent}}
#' @export
add_source_file<-function(metadata, filepath, code=NULL, flag.binary=FALSE, flag.checksum=TRUE)
{
  depwalker:::assertMetadata(metadata)
  checkmate::assert_logical(flag.checksum)
  filepath <- depwalker:::get.codepath(metadata, filepath)
  checkmate::assertPathForOutput(pathcat::path.cat(getwd(), dirname(metadata$path), filepath), overwrite=TRUE)
  if (file.exists(filepath))
  {
    if (!is.null(code)) {
      code<-normalize_code_string(code)
      # Sprawdzamy, czy istniejący plik ma ten sam kod
      existing_code<-readLines(filepath)
      if (code != existing_code)
      {
        stop(paste0("The code file ", filepath, " already exists, but with different contents."))
      }
    }
  } else {
    if (is.null(code))
    {
      stop("You must either specify filepath to the already existing source file, or put the source code in the code parameter.")
    }
    code<-normalize_code_string(code)
    writeLines(code, filepath)
    message(paste0("Written ", length(code), " lines into ", filepath, "."))
    flag.binary=FALSE
  }
  metadata<-append_extra_code(metadata, filepath, flag.checksum,flag.binary = flag.binary)
  return(metadata)
}

#' Function that appends file path to the metadata. It does no checking, it simply
#' updates the data structure.
append_extra_code<-function(metadata, filepath, flag.checksum, flag.binary)
{
  filepath=pathcat::make.path.relative(base.path =  pathcat::path.cat(getwd(), dirname(metadata$path)), target.path = filepath)

  if (is.null(metadata$extrasources))
  {
    extrasources<-list()
  } else {
    extrasources<-metadata$extrasources
  }
  extrasources[filepath]<-list(list(path=filepath, flag.checksum=flag.checksum, flag.binary=flag.binary))
  metadata$extrasources<-extrasources
  return(metadata)
}
