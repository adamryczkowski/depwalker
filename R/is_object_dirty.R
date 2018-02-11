
#' Returns reasons why that cached value is stale and the object needs to be recalculated
#'
#' @param metadata The metadata object itself. It makes sure, that this metadata is saved to disk.
#' @return Returns a list with items naming all reasons why the cached value is stale. Each reason has its own list
#'         element, with elements \code{verdict} and \code{info}.
#'
#' @section Return value explanation:
#' Each element of the returned list is itself a list with the following elements:
#' \describe{
#' \item{\strong{verdict}}{saying \code{FALSE} meaning "ok, this is not a reason the task's cache is stale' and \code{TRUE}
#'         meaning opposite and \code{NA} meaning that we cannot say anything, because the task in memory doesn't contain that information,
#'         which could either mean that this information was removed because the task was changed, or that this task was re-created from
#'         scratch, in which case it may or may not be changed.}
#' \item{\strong{info}}{with additional information that might be usefull to the user}
#'}
#' Here is a list of all possible reasons:
#' \describe{
#' \item{\strong{too_many_parents}}{The task for which computations were done has more parents than our task.
#'                                  \code{info} contain a list of those parents. }
#' \item{\strong{too_little_parents}}{The task for which computations were done has less parents than our task.
#'                                    \code{info} contain a list of those missing parents. }
#' \item{\strong{parents_mismatch}}{Named list with reasons of mismatch for each parent.
#'                                  Some of the parents' definitions have changed since the task has been run the last time.
#'                                  \code{info} contain a list of those changed parents.}
#' \item{\strong{too_many_runtime_objects}}{The task for which computations were done has more input runtime objects than our task.
#'                                  \code{info} contain a list of those objects }
#' \item{\strong{too_little_runtime_objects}}{The task for which computations were done has less runtime objects than our task.
#'                                    \code{info} contain a list of those missing runtime objects. }
#' \item{\strong{runtime_objects_mismatch}}{Named list with reasons of mismatch for each runtime object.
#'                                  Some of the runtime objects have been replaced/changed since the task has been run the last time.
#'                                  \code{info} contain a list of those changed objects.}
#' \item{\strong{too_many_source_files}}{The task for which computations were done has more source files than our task.
#'                                  \code{info} contain a list of those files }
#' \item{\strong{too_little_source_files}}{The task for which computations were done has less source files than our task.
#'                                    \code{info} contain a list of those missing source files. }
#' \item{\strong{source_files_mismatch}}{Named list with reasons of mismatch for each source file. Main source file has a name '/'.
#'                                  Some of the source files have been replaced/changed since the task has been run the last time.
#'                                  \code{info} contain a list of those changed files.}
#' \item{\strong{too_many_libraries}}{The task for which computations were done has more libraries as dependency than our task.
#'                                  \code{info} contain a list of those libraries}
#' \item{\strong{too_little_libraries}}{The task for which computations were done has less libraries as dependency than our task.
#'                                    \code{info} contain a list of those missing dependencies. }
#' \item{\strong{libraries_mismatch}}{Different libraries versions.
#'                                  \code{info} contain a list of those mismatces.}
#'}
#'
why_cached_value_is_stale<-function(m) {
  out<-list()
  actual_hashes<-calculate_task_state_digest(m, flag_full_hash = TRUE)



  if(length(actual_hashes$parents) != length(m$parents)) {
    more<-setdiff(names(actual_hashes$parents), names(m$parents))
    less<-setdiff(names(m$parents), names(actual_hashes$parents))
    if(length(more)>0) {
      out$too_many_parents<-list(verdict=TRUE, info=more)
    } else {
      out$too_many_parents<-list(verdict=FALSE)
    }
    if(length(less)>0) {
      out$too_little_parents<-list(verdict=TRUE, info=less)
    } else {
      out$too_little_parents<-list(verdict=FALSE)
    }
  } else {
    out$too_little_parents<-list(verdict=FALSE)
    out$too_many_parents<-list(verdict=FALSE)
  }

  if(length(m$parents)>0) {
    pm<-list()
    verdict<-FALSE
    for(i in seq_along(actual_hashes$parents)) {
      pname<-names(actual_hashes$parents)[[i]]
      p_disk<-actual_hashes$parents[[pname]]
      p<-m$parents[[pname]]
      if('digest' %in% names(p)){
        if(p$digest != p_disk$hash) {
          pm[[pname]]<-list(verdict=TRUE)
          if(!is.na(verdict)){
            verdict<-TRUE
          }
        } else {
          pm[[pname]]<-list(verdict=FALSE)
        }
      } else{
        pm[[pname]]<-list(verdict=NA)
        verdict<-NA
      }
    }
    out$parents_mismatch<-list(verdict=verdict, info=pm)
  } else {
    out$parents_mismatch<-list(verdict=FALSE)
  }


  if(length(actual_hashes$runtime) != length(m$inputobject)) {
    more<-setdiff(names(actual_hashes$runtime), names(m$inputobject))
    less<-setdiff(names(m$inputobject), names(actual_hashes$runtime))
    if(length(more)>0) {
      out$too_many_runtime_objects<-list(verdict=TRUE, info=more)
    } else {
      out$too_many_runtime_objects<-list(verdict=FALSE)
    }
    if(length(less)>0) {
      out$too_little_runtime_objects<-list(verdict=TRUE, info=less)
    } else {
      out$too_little_runtime_objects<-list(verdict=FALSE)
    }
  } else {
    out$too_little_runtime_objects<-list(verdict=FALSE)
    out$too_many_runtime_objects<-list(verdict=FALSE)
  }

  if(length(m$inputobject)>0) {
    io<-list()
    verdict<-FALSE
    inputobjects_df<-depwalker:::lists_to_df(m$inputobjects, list_columns=c('name', 'objectdigest', 'size'))
    inputobjects_df<-tidyr::unnest(inputobjects_df)
    for(i in seq_along(actual_hashes$runtime)) {
      objname<-names(actual_hashes$runtime)[[i]]
      o_disk<-actual_hashes$runtime[[objname]]
      o<-as.list(inputobjects_df[which(inputobjects_df$name==objname),])
      if('objectdigest' %in% names(o)){
        if(o$objectdigest != o_disk$hash) {
          io[[objname]]<-list(verdict=TRUE)
          if(!is.na(verdict)){
            verdict<-TRUE
          }
        } else {
          io[[objname]]<-list(verdict=FALSE)
        }
      } else{
        io[[objname]]<-list(verdict=NA)
        verdict<-NA
      }
    }
    out$runtime_mismatch<-list(verdict=verdict, info=io)
  } else {
    out$runtime_mismatch<-list(verdict=FALSE)
  }



  if(length(actual_hashes$code) != length(m$extrasources)+1) {
    more<-setdiff(names(actual_hashes$code), c('/', names(m$extrasources)))
    less<-setdiff(c('/', names(m$extrasources)), names(actual_hashes$code))
    if(length(more)>0) {
      out$too_many_source_files<-list(verdict=TRUE, info=more)
    } else {
      out$too_many_source_files<-list(verdict=FALSE)
    }
    if(length(less)>0) {
      out$too_little_source_files<-list(verdict=TRUE, info=less)
    } else {
      out$too_little_source_files<-list(verdict=FALSE)
    }
  } else {
    out$too_little_source_files<-list(verdict=FALSE)
    out$too_many_source_files<-list(verdict=FALSE)
  }

  #Main code
  cd<-list()
  verdict<-FALSE
  # if(actual_hashes$code[['/']]$digest!=calculate_one_digest(m$code)) {
  #   verdict<-TRUE
  # } else {
  #   verdict<-FALSE
  # }
  # cd[['/']]<-list(verdict=verdict)

  if(length(m$extrasources)>0) {
    for(i in seq_along(actual_hashes$code)) {
      codename<-names(actual_hashes$code)[[i]]
      c_disk<-actual_hashes$code[[codename]]
      if(codename==m$codepath) {
        c_mem<-list(path=m$codepath,
                    flag.checksum=TRUE,
                    flag.binary=FALSE,
                    flag.r=TRUE,
                    digest=m$codeCRC)
      } else {
        c_mem<-m$extrasources[[codename]]
      }
      if('digest' %in% names(c_mem)){
        if(c_mem$digest != c_disk$hash) {
          cd[[codename]]<-list(verdict=TRUE)
          if(!is.na(verdict)){
            verdict<-TRUE
          }
        } else {
          cd[[codename]]<-list(verdict=FALSE)
        }
      } else{
        cd[[codename]]<-list(verdict=NA)
        verdict<-NA
      }
    }
    out$source_files_mismatch<-list(verdict=verdict, info=cd)
  } else {
    out$source_files_mismatch<-list(verdict=FALSE)
  }









  if(length(actual_hashes$libraries) != length(m$libraries)) {
    more<-setdiff(names(actual_hashes$libraries), names(m$libraries))
    less<-setdiff(names(m$libraries), names(actual_hashes$libraries))
    if(length(more)>0) {
      out$too_many_libraries<-list(verdict=TRUE, info=more)
    } else {
      out$too_many_libraries<-list(verdict=FALSE)
    }
    if(length(less)>0) {
      out$too_little_libraries<-list(verdict=TRUE, info=less)
    } else {
      out$too_little_libraries<-list(verdict=FALSE)
    }
  } else {
    out$too_little_libraries<-list(verdict=FALSE)
    out$too_many_libraries<-list(verdict=FALSE)
  }



  return(out)
}

#' Returns whether the cached value is stale
#'
#' @param metadata The metadata object itself.
#' @return Returns \code{TRUE} if cached value is stale, \code{FALSE} if it isn't and
#' \code{NA} if it cannot decide (maybe because the statistics were overwritten).
#'
is_cached_value_stale<-function(m) {
  ans<-why_cached_value_is_stale(m)

  # nested_items<-c('parents_mismatch', 'runtime_objects_mismatch', 'source_files_mismatch')
  # nonnested_items<-setdiff(names(ans), nested_items)
  # out<-FALSE
  for(nni in names(ans)) {
    value<-ans[[nni]]$verdict
    if(is.na(value)){
      return(NA)
    }
    if(value==TRUE){
      out<-TRUE
    }
  }
  # for(ni in nested_items) {
  #   dic<-ans[[ni]]
  #   for(i in names(dic)) {
  #     value<-dic[[i]]
  #     if(is.na(value)){
  #       return(NA)
  #     }
  #     if(value==TRUE){
  #       out<-TRUE
  #     }
  #   }
  # }
  return(out)
}


#Calculates the actual task state hash, that can be used to infer if its definition has changed and there is a need to recalculate all descendants
#It is a list
calculate_task_state_digest<-function(m, flag_full_hash=FALSE, flag_include_objectrecords=FALSE) {
  #The function gets the following pieces of the puzzle:
  # If the task_state_digest executed on each parent gives the same digest as recorded in the metadata
  out_parents<-list()
  if(length(m$parents)>0) {
    parents<-m$parents[order(names(m$parents))]
    for(p in parents) {
      path<-get.parentpath(p, m, flag_include_extension=FALSE)
      ans<-tryCatch(
        load_metadata(path),
        error=function(e){e}
      )
      if('error' %in% ans) {
        out_parents[[p$name]]<-ans
      } else {
        parenthash<-calculate_task_state_digest(ans, flag_full_hash = FALSE, flag_include_objectrecords=TRUE)
        ans<-paste0(parenthash, ': ', paste0(p$name, '->', p$aliasname, collapse=','))
        out_parents[[p$name]]<-digest::digest(ans, serialize = FALSE)
      }
    }
  }


  # If the runtime.environment objects are the same, as recorded in the metadata

  # m$inputobjects<-list(
  #   obj1_path=list(
  #     name=c('a','b', 'c'), ignored=FALSE, path='obj1_path', compress='xz',
  #     objectdigest=c('DIGEST1','DIGEST2','DIGEST3'), filedigest='FILEDIGEST1',
  #     size=c(80,34,45), filesize=108, mtime=file.mtime('/etc/fstab')),
  #   obj2_path=list(
  #     name=c('obj'), ignored=FALSE, path='obj2_path', compress='xz',
  #     objectdigest=c('DIGEST4'), filedigest='FILEDIGEST2',
  #     size=654, filesize=438, mtime=file.mtime('/etc/fstab')),
  #   obj3_path=list(
  #     name='dt', ignored=TRUE)
  # )

  if(length(m$inputobjects)>0) {
    inputobjects_df<-depwalker:::lists_to_df(m$inputobjects, list_columns=c('name', 'objectdigest', 'size'))
    inputobjects_df<-purrrlyr::by_row(inputobjects_df, ~length(.$name[[1]]), .collate = 'cols', .to='count')
    inputobjects_df<-dplyr::arrange(tidyr::unnest(inputobjects_df), name)

    obj_container<-new.env()
    inputobjects_df$hash<-NA_character_
    filehashes<-list()

    if(nrow(inputobjects_df)>0) {
      for(i in seq(1, nrow(inputobjects_df))) {
        if(!inputobjects_df$ignored[[i]]) {
          path<-depwalker:::get.fullpath(m, path=inputobjects_df$path[[i]])
          if(file.exists(path)) {
            if(!'path' %in% names(filehashes)){
              filehashes[[path]]<-tools::md5sum(path)
            }
            hash<-filehashes[[path]]
            if(hash!=inputobjects_df$filedigest[[i]]) {
              if(inputobjects_df$count[[i]]>1) {
                if(!inputobjects_df$name[[i]] %in% obj_container) {
                  objs<-readRDS(path)
                  if(!'list' %in% class(objs)) {
                    stop(paste0("Wrong format of the ", path, ". Expected class list"))
                  }
                  for(i in seq_along(objs)) {
                    objname<-names(objs)[[i]]
                    assign(x = objname, value = objs[[objname]], envir = obj_container)
                  }
                  rm('objs')
                }
                if(inputobjects_df$name[[i]] %in% obj_container) {
                  inputobjects_df$hash[[i]]<-calculate.object.digest(inputobjects_df$name[[i]], target.environment=obj_container)
                  rm(inputobjects_df$name[[i]], envir = obj_container)
                } else {
                  stop(paste0("Cannot find object ", inputobjects_df$name[[i]], " in ", inputobjects_df$path[[i]]))
                }
              }
            } else {
              inputobjects_df$hash[[i]]<-'OK'
            }
          }
        }
      }
    }
    rm(obj_container)
    out_runtime<-as.list(setNames(inputobjects_df$hash, inputobjects_df$name))
  } else {
    out_runtime<-list()
  }


  if(length(libraries)>0) {
    libraries_df<-objectstorage::lists_to_df(m$libraries)
    #The smart line below makes cannonical form of the priorities, be retaining only
    #the ordering. It is very similar to rank, but makes the values integers (without fractional part)
    libraries_df$priority<-as.integer(as.factor(libraries_df$priority))
    libraries_df<-dplyr::arrange(libraries_df, name)
    out_libraries<-list()
    out<-rep('', nrow(libraries_df))
    for(i in seq(nrow(libraries_df))) {
      ans<-paste0(libraries_df$priority[[i]], ":", libraries_df$name[[i]],
                  "@", libraries_df$version[[i]])
      out[[i]]<-digest::digest(ans, serialize = FALSE)
    }
    libraries_df$hash<-out
    out_libraries<-as.list(setNames(libraries_df$hash, libraries_df$name))
  } else {
    out_libraries<-list()
  }

  # If the code digest of each input file is the same as recorded in the metadata

  # if('codepath' %in% names(m)) {
  #   code<-readLines(depwalker:::get.fullpath(metadata = m, m$codepath))
  #   out_code<-list('/'=list(hash=calculate_one_digest(code)))
  # } else {
  #   out_code<-list()
  # }
  out_code<-list()
  code_files<-depwalker:::get_coding_files(m, flag_expand_paths = FALSE)
  for(i in seq_along(code_files)) {
    filename<-code_files[[i]]
    path<-pathcat::path.cat(dirname(m$path), filename)
    if(!file.exists(path)) {
      hash<-simpleError(paste0("Cannot find source text file ", path, "."))
    } else {
      hash<-paste0(basename(path), ":", source_file_digest(path))
    }
    out_code[[filename]]<-list(hash=hash)
  }

  binary_files<-depwalker:::get_binary_files(m, flag_expand_paths = FALSE)
  for(i in seq_along(binary_files)) {
    filename<-binary_files[[i]]
    path<-pathcat::path.cat(dirname(m$path), filename)
    if(!file.exists(path)) {
      hash<-simpleError(paste0("Cannot find source binary file ", path, "."))
    } else {
      hash<-tools::md5sum(path)
    }
    out_code[[filename]]<-list(hash=hash)
  }

  out_code<-out_code[order(names(out_code))]


  if(flag_include_objectrecords) {
    objnames<-names(m$objectrecords)
    idx<-order(objectnames)
    objnames<-objnames[idx]
    txt<-paste0(objnames, collapse = ',')
    d<-digest::digest(txt, serialize=FALSE)
    checkmate::assertString(d)
    out_objrec<-d
    ans<-list(parents=out_parents,
              runtime=out_runtime,
              code=out_code,
              libraries=out_libraries,
              objectrecords=d)
  } else {
    ans<-list(parents=out_parents,
              runtime=out_runtime,
              code=out_code,
              libraries=out_libraries)
  }

  if(length(m$libraries)>0) {

  }

  if(flag_full_hash) {
    return(ans)
  } else {
    return(digest::digest(ans))
  }
}

#' Returns corrected metadata if code has been changed by external force, forcing us to do
#' recalculation. NULL otherwise
code_has_been_changed<-function(metadata)
{
  digests<-calculate_code_digest(metadata)
  if (is.null(metadata$codeCRC) || metadata$codeCRC != digests)
  {
      metadata$codeCRC <- digests
      return(metadata)
  } else {
    return(NULL)
  }
}

#' Calculates R code component of the task's metadata.
#'
#' If there is more than one line of code, it calculates digest for each line separatedly,
#' then digests the final digest of concatenation of all individual digests.
#' If there is only one line of code it returns digest of it.
#'
#' @param metadata Metadata of the object
calculate_code_digest<-function(metadata)
{
  files<-get_coding_files(metadata, flag_expand_paths = TRUE)
  if (is.null(files))
  {
    digests<-calculate_one_digest(metadata$code)
  } else {
    digests<-plyr::aaply(files,1,source_file_digest)
  }

  files<-get_binary_files(metadata, flag_expand_paths = TRUE)
  if (!is.null(files))
  {
    digests2<-plyr::aaply(as.character(files),1,tools::md5sum)
    digests<-c(digests,digests2)
  }
  if (length(digests)>1)
  {
    return(digest::digest(paste0(digests,collapse=''), serialize = FALSE))
  } else {
    return(digests)
  }
}
