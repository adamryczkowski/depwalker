#' Creates task's metadata object.
#'
#' The function will create basic metadata describing the the object recipe.
#' @param code Character vector with code. Each element of the vector will be treated as separate line.
#' @param source.path Alternative way of specifying code to run. \code{code} and \code{source.path} are mutually exclusive, and required.
#' @param metadata.path path to the task's metadata file without extension. The metadata will not be saved with this command, but
#'    the path information is nevertheless required, as it is a mandatory part of the task's metadata specification.
#'    If path is relative, it will be assumed to be relative to the current working directory.
#' @param flag.never.execute.parallel if set, the task will never trigger parallel execution of dependent tasks. Defaults to \code{FALSE}.
#' @param execution.directory directory relative to the metadata.path, which will be set as the current directory when the
#'    code is run.
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


  metadata<-list(code=code, path=metadata.path, parents=list(), objectrecords=list(), inputobjects=list(),
                 flag.never.execute.parallel=flag.never.execute.parallel,
                 flag.force.recalculation=FALSE, execution.directory=execution.directory)
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
#' @param metadata child metadata you wish to add ancestor to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param parent metadata with the dependency object. Child will remember the relative path to the object, even if it is not saved to disk yet.
#' @param parent.path either relative to path of \emph{child metadata} object or full path with the
#'   ancestor's metadata file.
#' @param flag_remember_absolute_path If set, the parent will be remembered by its absolute path, rather than relative (default)
#' @param name name of the object's name in the parent task you with to import.
#' @param aliasname optional argument. If set it specifies alternate name of the imported
#'   \code{name} object as it is used by the task's R code. This setting allows great
#'   flexibility in naming task's output objects.
#' @return modified \code{metadata} argument that includes specified parent's record.
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.objectrecord}}
add.parent<-function(metadata=NULL, name=NULL, parent.path=NULL,  parent=NULL, aliasname=NULL, flag_remember_absolute_path=FALSE,
                     flag_overwrite_parent=FALSE)
{
  assertMetadata(metadata)
  if(is.null(parent) && is.null(parent.path)){
    stop("You must provide either parent or parent.path")
  }

  if(!is.null(parent.path) && !is.null(parent)) {
    if (parent$path != parent.path) {
      stop(paste0("Ambivalent options encountered: parent.path (",parent.path,
                  ") and the parent object, that points to the different directory: ", parent$path))
    }
  }

  if(!is.null(parent)) {
    assertMetadata(parent)
    parent.path <- parent$path
  }

  if(!is.null(parent.path)) {
    assertValidPath(parent.path)
    parent<-load.metadata(parent.path)
  }


  if (is.null(name) && is.null(aliasname))
  {
    name<-purrr::map_chr(parent$objectrecords, 'name')
    aliasname<-name
  } else {
    if(is.null(aliasname)) {
      aliasname<-name
    }
    if(is.null(name)) {
      name<-purrr::map_chr(parent$objectrecords, 'name')
    }
  }
  if(length(aliasname)!=length(name)) {
    stop("Length of aliasname and name must be the same. In name is not specified, aliasname must have length equal to the number of exported objects")
  }


  assertVariableNames(aliasname)
  assertVariableNames(name)

  parents<-metadata$parents
  parentnames<-unlist(purrr::map(parents, ~.$aliasname))

  if(length(parentnames)>0) {
    if (sum(aliasname %in% parentnames)>0)
    {
      tmp<-aliasname %in% parentnames
      stop(paste0(paste0(aliasname[[tmp]], collapse=', '), ' is already present in parents of ', metadata$path))
      #stop(paste0('Object(s) ', paste0(aliasname[[tmp]], collapse=', '), " is/are already present in parents of ", metadata$path))
    }
  }


  path<-pathcat::path.cat(getwd(), dirname(metadata$path), parent.path)
  if(!flag_remember_absolute_path) {
    path<-pathcat::make.path.relative(base.path =  pathcat::path.cat(getwd(), dirname(metadata$path)),
                                      target.path = path)
  }
  new_parent<-list(name=name, path=path, aliasname=aliasname)

  if(path %in% names(parents) && !flag_overwrite_parent) {
    if(!identical(new_parent, parents[[path]])) {
      stop("Parent ", path, " is already present in the list of parents")
    }
  }
  parents[[path]]<-new_parent
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
  path=pathcat::make.path.relative( base.path = pathcat::path.cat(getwd(), dirname(metadata$path)), target.path = path)

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
add_source_file<-function(metadata, filepath, code=NULL, flag.binary=FALSE, flag.r=NULL, flag.checksum=TRUE)
{
  if(is.null(flag.r)) {
    if(flag.binary) {
      flag.r = FALSE
    } else {
      flag.r = TRUE
    }
  }
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
  metadata<-append_extra_code(metadata, filepath, flag.checksum,flag.binary = flag.binary, flag.r = flag.r)
  return(metadata)
}

#' Function that appends file path to the metadata. It does no checking, it simply
#' updates the data structure.
append_extra_code<-function(metadata, filepath, flag.checksum, flag.binary, flag.r)
{
  filepath=pathcat::make.path.relative(base.path =  pathcat::path.cat(getwd(), dirname(metadata$path)), target.path = filepath)

  if (is.null(metadata$extrasources))
  {
    extrasources<-list()
  } else {
    extrasources<-metadata$extrasources
  }
  extrasources[filepath]<-list(list(path=filepath, flag.checksum=flag.checksum, flag.binary=flag.binary, flag.r=flag.r))
  metadata$extrasources<-extrasources
  return(metadata)
}


#' Add additional objects to be available when the task is run. Objects will be serialized to disk when
#' metadata is saved.
#' By default all small objects will kept together, and large objects will live in a separate files.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param objects Either list or environment with the objects.
#' @param forced_save_filenames List or named character vector that forces save of the specific object (given in key) into the filepath given in value.
#'        It overrides the built in algorithm of naming and grouping the objects together in the serialized archives.
#' @param save_location Path to the alternate save location. By default the save location is the same as
#'        save location of the metadata objects, when only one file will be created, and subdirectory
#'        \code{<task_name>_runtime_objects} when more than one file will be automatically created.
#' @param forced_separate_files Character vector with the list of objects that will be forcefully serialized
#'        into a separate, dedicated archive
#' @param compress Compress method to use. Defaults to gzip.
#' @param .ignored_objects Expert argument. Character vector with the list of objects present
#'        in the \code{objects} argument, but which presence will not be tracked and saved.
#'        The code must be written in such a way, that it doesn't rely on the existance and validity
#'        of this object, e.g. this object can be used to speed things up.
#' @return modified \code{metadata} argument that includes additional runtime objects definition. Nothing will get
#'        actually saved to disk (for this use make.sure.metadata.is.saved)
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.parent}}
#' @export
add_runtime_objects<-function(metadata, objects, save_location=NULL, forced_save_filenames=character(0),
                              forced_separate_files, compress='gzip', .ignored_objects=character(0))
{
  if('list' %in% class(objects)) {
    objects<-as.environment(objects)
  }
  if(!'inputobjects' %in% names(metadata)) {
    metadata$inputobjects<-list()
  }


  #First we add ignored objects, as there is the least amount of work to do with them
  objectnames<-intersect(names(objects), .ignored_objects)
  if(length(objectnames)>0) {
    for(on in objectnames) {
      metadata$inputobjects[[on]]<-list(name=on, ignored=TRUE)
    }
  }

  #Now we focus on the non-ignored objects
  objectnames<-setdiff(names(objects), .ignored_objects)

  objectsizes<-purrr::map_dbl(objectnames, ~object.size(objects[[.]]))

  #Objects bigger than 5kb will be put in the separate containers.
  #separate_containers is a flag for each object specifying whether to use a separate container for it.
  separate_containers<-objectnames %in% c(names(forced_save_filenames), forced_separate_files) |
                       objectsizes > getOption('tune.threshold_objsize_for_dedicated_archive')

  number_of_files<-sum(separate_containers)

  if(number_of_files==0) {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects.rds")
  } else {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects/default_container.rds")
    separate_objects<-objectnames[separate_containers]
    separate_paths<-paste0(basename(metadata$path), "_runtime_objects/_", separate_objects, ".rds")
    if(length(forced_separate_files)>0) {
      for(i in seq(1, length(forced_separate_files))) {
        obj_name<-names(forced_separate_files)[[i]]
        if(obj_name %in% names(forced_separate_files)){
          path<-forced_separate_files[[obj_name]]
          pos<-which(obj_name == separate_objects)
          separate_paths[[pos]]<-path
        }
      }
    }
  }
  jobs<-list()

  if(any(!separate_containers)) {
    #Create entries for each small object in the shared container
    relative_path<-path=pathcat::make.path.relative(metadata$path, generic_file_name)
    objnames<-objectnames[which(!separate_containers)]
    objsizes<-objectsizes[which(!separate_containers)]
    objdigests<-purrr::map_chr(objnames, ~digest::digest(objects[[.]]))

    metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                      path=relative_path,
                                      compress=compress, objectdigest=objectdigest,
                                      size=objsizes)
  }
  if(any(separate_containers)) {
    poss<-which(separate_containers)
    for(pos in poss) {
      objectname<-objectnames[[pos]]
      objsize<-objectsizes[[pos]]
      relative_path<-path=pathcat::make.path.relative(metadata$path, separate_paths)
      objdigest<-digest::digest(objects[[objectname]])


      metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                        path=relative_path,
                                        compress=compress, objectdigest=objectdigest,
                                        size=objsize)
    }
  }

  return(metadata)
}


