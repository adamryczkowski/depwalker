#' Creates task's metadata object.
#'
#' The function will create basic metadata describing the the object recipe.
#' @param code Character vector with code. Each element of the vector will be treated as separate line.
#' @param source.path Alternative way of specifying code to run. \code{code} and \code{source.path} are mutually exclusive, and required.
#' @param metadata.path path to the task's metadata file without extension. The metadata will not be saved with this command, but
#'    the path information is nevertheless required, as it is a mandatory part of the task's metadata specification.
#'    If path is relative, it will be assumed to be relative to the current working directory.
#' @param flag.never.execute.parallel if set, the task will never trigger parallel execution of dependent tasks. Defaults to \code{FALSE}.
#' @param flag.use.tmp.storage Boolean flag. If set, it indicates that the task's path is slow to write, and
#'       all large uncompressed files will be first saved into the \code{/tmp}, and then saved into the
#'       destination path. Defaults to FALSE
#' @param execution.directory directory relative to the metadata.path, which will be set as the current directory when the
#'    code is run.
#' @return task's metadata object. One might want to complete object's creation with complementary functions
#'    \code{add.parent} and \code{add.object.record}
#' @export
#' @seealso \code{\link{add.parent}}, \code{\link{add.objectrecord}}
create_metadata<-function(name,
                          inputobjects_storage_path=NULL,
                          objectrecords_storage_path=NULL,
                          runtime_environment=NULL,
                          execution_directory='',
                          flag_never_execute_parallel=FALSE,
                          flag_use_tmp_storage=NULL)
{
  checkmate::assert_string(name)
  checkmate::assert_true(name!='')
  metadata<-list(path=name)
  path<-get_path(metadata, name, extension='metadata')
  checkmate::assertPathForOutput(dirname(path), overwrite=TRUE)
  metadata$path<-pathcat::file_path_sans_all_ext(name)


  metadata$inputfiles<-list()
  metadata$inputobjects<-list()

  if(is.null(inputobjects_storage_path)) {
    inputobjects_storage_path<-paste0(basename(metadata$path), '_io')
  }
  path_to_check<-get_path(metadata, inputobjects_storage_path, input_relative_to='metadata',
                          extension='objectstorage')
  checkmate::assertPathForOutput(dirname(path_to_check), overwrite=TRUE)
  inputobjects_storage_path<-get_path(metadata, inputobjects_storage_path, input_relative_to='metadata',
                                      extension='ignore', return_relative_to='metadata')
  metadata$inputobjects_storage<-inputobjects_storage_path

  metadata$parents<-list()

  metadata$objectrecords<-list()

  if(is.null(objectrecords_storage_path)) {
    objectrecords_storage_path<-paste0(basename(metadata$path), '_or')
  }
  path_to_check<-get_path(metadata, inputobjects_storage_path, input_relative_to='metadata',
                          extension='objectstorage')
  checkmate::assertPathForOutput(dirname(path_to_check), overwrite=TRUE)

  objectrecords_storage_path<-get_path(metadata, inputobjects_storage_path, input_relative_to='metadata',
                                      extension='ignore', return_relative_to='metadata')
  metadata$objectrecords_storage<-objectrecords_storage_path

  runtime_environment<-new.env()
  metadata$runtime_environment<-runtime_environment

  path_to_check<-get_path(metadata, execution_directory)
  checkmate::expect_directory_exists(path_to_check, access='w')
  execution_directory<-get_path(metadata, execution_directory, return_relative_to='metadata')
  metadata$execution_directory<-execution_directory

  checkmate::assertFlag(flag_never_execute_parallel)
  metadata$flag_never_execute_parallel<-flag_never_execute_parallel

  checkmate::assertFlag(flag_use_tmp_storage)
  metadata$flag_use_tmp_storage<-flag_use_tmp_storage

  checkmate::assertDataFrame(metadata$timecosts)
  cols<-c('walltime', 'cputime', 'systemtime','cpumodel', 'membefore',
          'memafter', 'corecount', 'virtualcorecount', 'busycpus')
  if (!all(cols %in% colnames(metadata$timecosts)))
    stop("Insufficient columns in metadata$timecosts data.frame")

  timecosts<-list()
  metadata$timecosts<-timecosts
  assertMetadata(metadata)
  return(metadata)
}
#' Add additional source file to the task.
#'
#' Nothing will get saved to disk. For that use \code{\link{save_metadata}}.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param filepath Path to the file, if the file is already existing (it can be relative to the task's path).
#'        It can be ommited, if type=='RMain', then it is assumed to be \code{<taskname>.R}, and \cpde{code} must be set.
#' @param code Optional string with the contents of the file. If specified and the file does not already exist, the file will be created
#'        with this contents.
#' @param type Type of the source file. Possible values: \code{RMain} (default), \code{R}, \code{txt}, \code{binary}.
#'        When \code{binary} type, the file will be checksummed in binary mode rather than line-by-line. Checksumming line-by-line has the advantage of
#'        being independent on a way the newline character is encoded into the file.
#' @param flag_checksum If set (which is default), the file's contents will be examined to check for changes when evaluating freshness of this job.
#' @param flag_force_overwrite If set, no error will be given, if the code already exists in the given path but
#'        with different contents. It will be overwritten. Default FALSE
#' @param flag_store_abolute_path If set, it will store the absolute path in the metadata. Usefull if you want
#'        to move the metadata, but keep the code in the original place.
#' @return modified \code{metadata} argument that includes additional source file or NULL if error.
#' @export
#' @seealso \code{\link{create_metadata}}, \code{\link{add_parent}}, \code{\link{add_objectrecord}}
add_source_file<-function(metadata, filepath=NULL, code=NULL, type='RMain',
                          flag_checksum=TRUE, flag_store_abolute_path=FALSE,
                          flag_force_overwrite=FALSE)
{
  assertMetadata(metadata, flag_ready_to_run=FALSE)

  assertSourceType(type)

  if(type!='RMain') {
    if(is.null(filepath)){
      stop("filepath is required argument if type is different than RMain")
    }
  } else {
    if(is.null(filepath)){
      filepath<-basename(metadata$path)
    }
  }

  checkmate::assertFlag(flag_checksum)
  checkmate::assertFlag(flag_store_abolute_path)
  checkmate::assertFlag(flag_force_overwrite)

  path<-get_path(metadata, filepath)
  if(!file.exists(path)){
    if(is.null(code)) {
      if(flag_checksum) {
        stop(paste0("There is no file in ", path))
      } else {
        warning(paste0("There is no file in ", path, ". "))
      }
    }
  }

  #Check and normalize the code
  if(!is.null(code)) {
    if(type=='binary') {
      if(!is.raw(code)) {
        stop("code argument must of type raw if the file type is binary")
      }
    } else if(type=='R' || type=='RMain') {
      code<-normalize_code_string(code)
    } else if(type=='txt') {
      code<-normalize_text_string(code)
    } else {
      stop("Unkown code type")
    }
  }

  #If both code and the source file exist, check if they match.
  if(file.exists(path)) {
    hash2<-tools::md5sum(path)
  } else {
    hash2<-''
  }
  if(!is.null(code)) {
    hash1<-calculate_one_digest(code)
  } else {
    hash1<-''
  }


  if(hash1!=hash2) {
    #We need to write/overwrite the code
    if(hash1!='' && hash2!='' && !flag_force_overwrite) {
      stop(paste0("The code file ", path, " already exists, but with different contents."))
    }
    # if (file.exists(path)) {
    #   unlink(path)
    # }
    # if(type == 'binary') {
    #   writeBin(code, path)
    # } else {
    #   writeLines(code, path)
    # }
  }

  checkmate::assertPathForOutput(path, overwrite=FALSE)

  if(!flag_store_abolute_path) {
    path<-get_path(metadata, path, return_relative_to='metadata')
  }

  metadata<-append_extra_code(metadata, filepath=path, code=code, flag_checksum=flag_checksum, type = type, digest = digest)
  return(metadata)
}

#' Function that appends file path to the metadata. It does no checking, it simply
#' updates the data structure.
append_extra_code<-function(metadata, filepath, code, flag_checksum, type, digest)
{
  if(is.null(code)) {
    metadata$inputfiles[[filepath]]<-list(path=filepath, flag_checksum=flag_checksum, type=type,
                                          digest=digest)
  } else {
    metadata$inputfiles[[filepath]]<-list(path=filepath, code=code, flag_checksum=flag_checksum,
                                          type=type, digest=digest)
  }
  return(metadata)
}

#' Adds required object to the task directly
#'
#' There are exactly two ways of putting an R object in the tasks' memory prior its execution:
#'
#' \enumerate{
#' \item with \code{\link{add_parent}} to add a task that generates the value
#' \item with \code{\link{add_inputobject}} to add a value directly
#' }
#'
#' Adding a value directly only appends the value in the task's runtime environment, hence
#' it will occupy RAM memory. If you want to free it, save the task using \code{\link{make_sure_task_is_saved}},
#' and load the task with \code{\link{load_task}}.
#'
#' If the object's record with specified name already exists, it will be silently overwritten.
#' @param metadata already created metadata you wish to add existing object.
#' @param objectnames names of the new objects. If object doesn't exist in the environment, then it will be
#' removed from the metadata
#' @param environment Environment that contains all the \code{objectnames} objects.
#'   Thus the names of the object must match both \code{environment} and how the task references them.
#' @param ignored Boolean (either single value, or vector of the size of \code{objectnames} or named vector with
#'        \code{objectnames} as keys) that specified whether a given object should be saved to disk when
#'        saving metadata. Think about ignored objects as optionally required, given only for efficiency.
#'        \emph{Task's code cannot assume the ignored objects will be present in memory.}
#' @return modified \code{metadata} with the registered objects and their value.
#' @export
add_inputobject<-function(metadata, objectnames=NULL, envir, ignored,
                          flag_allow_replace=FALSE, flag_allow_remove=TRUE)
{
  if(is.null(objectnames)){
    objectnames<-ls(envir = envir)
  }
  assertMetadata(metadata, flag_ready_to_run=FALSE)
  checkmate::assertCharacter(objectnames)
  checkmate::assert_environment(envir)
  checkmate::assert_logical(ignored)

  if(length(setdiff(objectnames, ls(envir = envir)))>0) {
    if(!flag_allow_remove) {
      stop(paste0("Objects ", paste0(setdiff(objectnames, ls(envir = envir)), collapse = ', '),
                  " are missing from the input envir and flag_allow_remove=FALSE"))
    }
    browser()
    #Object is missing
    #Removing objects is not yet implemented.
  }

  ignored<-objectstorage::parse_argument(arg = ignored, objectnames = objectnames, default_value = FALSE)
  objectdigests<-rep(NA, length(objectnames))
  for(i in seq_along(objectnames)) {
    objectname<-objectnames[[i]]
    objectdigests[[i]]<-objectstorage::calculate.object.digest(objectname = objectname,
                                                               target.environment = envir)
  }
  objectdigests<-setNames(objectdigests, objectnames)

  allobjects_df<-get_runtime_objects(metadata, TRUE, TRUE)

  if(sum(objectnames %in% allobjects$objectname)>0) {
    #Musimy sprawdzić, czy użytkownik nie wprowadza konfliktu z parents
    pos_objs<-which(allobjects_df$objectname %in% objectnames)
    for(dup_pos in pos_objs) {
      #Iterujemy się po każdym z obiektów
      dup_obj<-allobjects_df$objectname[dup_pos]
      if(allobjects_df$parent[dup_pos]){
        stop(paste0("The object ", dup_obj, " will already be available from parent records"))
      } else {
        if(allobjects_df$digest!=objectdigests[[dup_obj]]) {
          if(!flag_allow_replace) {
            stop(paste0("Object ", dup_obj, " already exists in this metadata"))
          }
        }
      }
    }
  }

  for(objectname in objectnames) {
    assign(objectname, value = get(objectname, envir = envir), envir = metadata$runtime_environment)

    metadata$inputobjects[[objectname]]<-list(name=objectname,
                                              ignored=ignored[[objectname]],
                                              digest=objectdigests[[objectname]])
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
#' @param parent_path either relative to path of \emph{child metadata} object or full path with the
#'   ancestor's metadata file.
#' @param flag_remember_absolute_path If set, the parent will be remembered by its absolute path, rather than relative (default)
#' @param flag_overwrite_parent If set, there will be no error message when changing parent for a specific object
#' @param name name of the object's name in the parent task you with to import.
#' @param aliasname optional argument. If set it specifies alternate name of the imported
#'   \code{name} object as it is used by the task's R code. This setting allows great
#'   flexibility in naming task's output objects.
#' @return modified \code{metadata} argument that includes specified parent's record.
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.objectrecord}}
add_parent<-function(metadata=NULL, name=NULL, parent=NULL, aliasname=NULL, flag_remember_absolute_path=FALSE,
                     flag_overwrite_parent=FALSE)
{
  assertMetadata(metadata, flag_ready_to_run=FALSE)

  if('character' %in% class(parent)) {
    parent.path<-parent
    parent<-load_metadata(parent.path)
  } else {
    parent.path<-parent$path
  }

  assertMetadata(parent)
  assertValidPath(parent.path)

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


  if(flag_remember_absolute_path) {
    return_relative_to<-NA
  } else {
    return_relative_to<-"metadata"
  }
  path<-get_path(metadata, parent.path, return_relative_to=return_relative_to)

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
#' @param archivepath Optional. Location of the cached file with the computed object. The path can
#'   be absolute or relative to the metadata's path.
#' @param compress optional. You can specify the compression method of the resulting object.
#'   When used \code{'xz'} compression, the computer will try to compress it with the
#'   parallel \code{pxz} application, if it is available on the system.
#' @param flag_store_relative_path If set it will store the absolute path in the metadata. Important, when you
#'   want to move the metadata to different folder, and keep the objectrecord in the old place. Default: FALSE
#'   Makes sense only if \code{archivepath} parameter is used.
#' @return modified \code{metadata} argument that includes specified object's record.
#' @export
#' @seealso \code{\link{create.metadata}}, \code{\link{add.parent}}
#' @export
add_objectrecord<-function(metadata, name, archivepath=NULL, compress='xz', flag_store_abolute_path=FALSE)
{
  assertMetadata(metadata, flag_ready_to_run=FALSE)
  checkmate::assertString(name)



  if (!is.null(archivepath))
  {
    path<-get_path(metadata, archivepath, input_relative_to='metadata', extension='ignore', return_relative_to=NA)
    assertValidPath(path)
    checkmate::assertPathForOutput(path, overwrite=TRUE)
    if(!flag_store_abolute_path) {
      path<-get_path(metadata, archivepath, input_relative_to='metadata', extension='ignore', return_relative_to='metadata')
    }
  } else {
    if(flag_store_abolute_path) {
      stop("Input flag_store_abolute_path and no archivepath was given")
    }
    path<-NA
  }

  assertCompress(compress)



  objectrecords<-metadata$objectrecords
  objectnames<-sapply(objectrecords, function(x)x$name)
  if (name %in% objectnames)
  {
    #Już jest ta nazwa w parentrrecords... Najpierw usuwamy poprzednią
    warning(paste0('object "', name, '" is already present in the exports of the task. Overwriting.'))
    idx<-which(objectnames==name)
    objectrecords[idx]<-NULL
  }

  objectrecords[[name]]<-list(name=name, path=path, compress=compress)
  metadata$objectrecords<-objectrecords

  assertMetadata(metadata)
  return(metadata)
}



append_history_record<-function(metadata, timestamp, walltime, cputime, systemtime, cpumodel,
                                membefore, memafter, corecount, virtualcorecount,
                                output, flag_success) {
  el<-list(timestamp=as.numeric(timestamp),
           walltime=as.numeric(walltime),
           cputime=as.numeric(cputime),
           systemtime=as.numeric(systemtime),
           cpumodel=cpumodel,
           membefore=as.numeric(membefore), memafter=as.numeric(memafter),
           corecount=as.integer(corecount), virtualcorecount=as.integer(virtualcorecount),
           output=normalize_text_string(output),
           flag_success=flag_success)
  metadata$history<-list(metadata$history, list(el))
  return(metadata)
}
