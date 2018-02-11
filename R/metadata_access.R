#' Functions that access to different properties of the metadata
#' Returns full path to the code
#' @param metadata Metadata of the task
#' @param path Path to modify
#' @param input_relative_to Determines how to interprete relative \code{path}.
#'        One of the following: \code{metadata} (default), \code{execution_directory}, \code{nothing}.
#'        \code{nothing} will treat the path relative to the current directory
#' @param extension What extension to put at the end of the file? One of the following:
#'        \code{metadata}, \code{objectstorage}, \code{lock}, \code{err_output}, \code{output}, \code{nothing}
#'        If the \code{path} already has that extension, nothing will get appended.

#' @export
get_path<-function(metadata, path, input_relative_to='metadata', extension='ignore', return_relative_to=NA)
{
  if(input_relative_to=='metadata') {
    path <- pathcat::path.cat(getwd(), dirname(metadata$path), path)
  } else if(relateive_to=='execution_directory') {
    path <- pathcat::path.cat(getwd(), dirname(metadata$path), metadata$execution.directory, path)
  } else if(relateive_to=='nothing') {
    path <- pathcat::path.cat(getwd(), path)
  } else {
    stop(paste0("Unkown input_relative_to type: ", input_relative_to))
  }

  if(!is.na(return_relative_to)) {
    if(return_relative_to=='metadata') {
      path<-pathcat::make.path.relative(get_path(metadata, ''), target.path = path)
    } else {
      stop(paste0("Unkown return_relative_to type: ", return_relative_to))
    }
  }
  path<-normalizePath(path, mustWork = FALSE)

  if(extension=='ignore') {
    return(path)
  }

  if(extension=='metadata') {
    ext<-getOption('metadata.save.extension')
  } else if (extension=='objectstorage') {
    ext<-getOption('objectstorage.index_extension')
  } else if (extension=='lock') {
    ext<-getOption('objectstorage.lock_extension')
  } else if (extension=='err_output') {
    ext<-getOption('depwalker.error_log_extension')
  } else if (extension=='output') {
    ext<-getOption('depwalker.log_extension')
  } else if (extension=='none') {
    ext<-''
  } else {
    stop(paste0("Unknown extesion type: ", extension))
  }
  if(ext!='') {
    ext2<-stringr::str_replace(ext, pattern=stringr::fixed('.'), replacement = '\\.')
    if(!stringr::str_detect(path, stringr::regex(paste0(ext2, '$')))) {
      path<-paste0(storagepath, ext)
    }
  }
  return(path)
}

#' @export
get_main_code<-function(metadata) {
  assertMetadata(metadata)
  inputdf<-get_inputfiles_as_df(metadata, list_columns='code')
  inputdf<-dplyr::filter(inputdf, type=='RMain')
  if(nrow(inputdf)!=1) {
    browser() #There must be exactly one record of type RMain
  }
  return(get_code(metadata, inputdf$path))
}

#' @export
get_code<-function(metadata, path) {
  assertMetadata(metadata)
  checkmate::assertString(path)
  inputdf<-get_inputfiles_as_df(metadata, list_columns='code')
  inputdf<-dplyr::filter(inputdf, path==path)
  if(nrow(inputdf)!=1) {
    browser() #There must be exactly one record of type RMain
  }
  if(is.na(inputdf$code)){
    path<-inputdf$path
    codepath<-get_path(metadata = metadata, path=path)
    if(inputdf$type=='binary') {
      code<-readBin(codepath, what='raw')
    } else {
      code<-readLines(codepath)
    }
  } else {
    code<-inputdf$code
  }
  return(code)
}

get_inputfiles_as_df<-function(metadata) {
  if(length(metadata$inputfiles)==0) {
    browser()
    stop("There must be at least one record in inputfiles")
  }
  df<-objectstorage::lists_to_df(metadata$inputfiles)
}

get_history_as_df<-function(metadata) {
  if(length(metadata$history)==0) {
    return(tibble::tibble(timestamp=numeric(0), walltime=numeric(0), cputime=numeric(0), systemtime=numeric(0),
                          cpumodel=character(0), membefore=numeric(0), memafter=numeric(0),
                          corecount=integer(0), virtualcorecount=integer(0),
                          output=list(), flag_success=logical(0)))
  } else {
    df<-objectstorage::lists_to_df(metadata$history, list_columns = 'output')
    return(df)
  }
}

get_last_history_statistics<-function(metadata) {
  df<-get_history_as_df(metadata)
  if(nrow(df)==0) {
    return(NULL)
  } else {
    return(as.list(dplyr::filter(dplyr::arrange(df, -timestamp), row_number()==1)))
  }
}

write_history_output_file<-function(metadata, flag_clear_history=FALSE) {
  last_history<-get_last_history_statistics(metadata)
  if(!is.null(last_history)) {
    if('output' %in% names(last_history)) {
      output<-normalize_text_string(last_history$output)

      if(last_history$flag_success) {
        output_path<-get_path(metadata=metadata, path=basename(metadata$path), extension='output')
      } else {
        output_path<-get_path(metadata=metadata, path=basename(metadata$path), extension='err_output')
      }
      if(file.exists(output_path)) {
        old_output<-readLines(output_path)
        if(digest::digest(output)==digest::digest(old_output)) {
          return() #No need to write, file is update
        }
      }
      writeLines(text = output, con = output_path)
    }
  }
}

acquire_lock<-function(metadata) {
  path<-get_path(metadata=metadata, basename(metadata$path), extension='lock')
  objectstorage::create.lock.file(path, timeout=getOption('depwalker.default_lock_time'))
}

release_lock<-function(metadata) {
  path<-get_path(metadata=metadata, basename(metadata$path), extension='lock')
  objectstorage::release.lock.file(path)
}

# l3<-list(a=3, p='file3', cz=Sys.time())
# l2<-list(a=2, b=paste0('string ', 1:10), p='file2')
# l1<-list(a=1, b=as.raw(1:100), p='file')
# l<-list('if'=list(file1=l1, file2=l2, file3=l3))

#Returns list of all the objects available at runtime.
#Returns data.frame with the following columns:
#objectname
#digest
#ignored - TRUE if the object is optional
#parent - TRUE if the source is parent, FALSE if inputobject
get_runtime_objects<-function(metadata, flag_include_parents=TRUE, flag_include_inputobjects=TRUE) {
  df<-tibble::tibble(objectname=character(0), digest=character(0), ignored=logical(0))
  if(flag_include_parents) {
    if(length(metadata$parents)>0) {
      dfp<-objectstorage::lists_to_df(metadata$parents, list_columns = c('names', 'aliasnames','objectdigests'))
      if(nrow(dfp)>0) {
        dfp<-tibble::tibble(tidyr::unnest(dfp), ignored=FALSE, parent=TRUE)
        df<-rbind(df, dplyr::select(dfp, objectname=aliasnames, digest=objectdigests, ignored))
      }
    }
  }
  if(flag_include_inputobjects) {
    if(length(metadata$inputobjects)>0) {
      dfi<-objectstorage::lists_to_df(metadata$inputobjects)
      if(nrow(dfi)>0) {
        dfi$parent=FALSE
        df<-rbind(df, dplyr::select(dfi, objectname=name, digest, ignored))
      }
    }
  }
  return(df)
}

is_inmemory<-function(m) {
  !pathcat::is_absolute_path(m$path)
}
