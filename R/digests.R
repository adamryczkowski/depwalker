#' Returns digest of the task's metadata.
#'
#' Digests are used in many places for easy identification of equivalent tasks' metadatas.
#'
#' Digest consists of all things, that are essential to define what the task is calculating.
#'
#' The digest is calculated on the task's essential properties:
#' \itemize{
#'  \item \strong{parent records}.
#'    \describe{
#'      \item{\code{digest}}{hash of the parent task, that include his objectrecords}
#'      \item{\code{name}}{name of the R object calculated by the ancestor}
#'      \item{\code{aliasname}}{optional. Specifies alternate name of the object for our R script}
#'    }
#'  \item \strong{code}.
#'    hash of contents of each source file with its filename (excluding directory)
#'  \item \strong{input objects} {}
#'  \item \strong{output object records}
#'    \describe{
#'      \item{\code{name}}{name of the output object}
#'    }
#' }
#'
#' @param metadata task's metadata object, based on which calculate the digest.
#'
#' The digest compirses the following items:
#'
#' \enumerate{
#' \item names and aliasnames of all parents
#' \item code
#' \item names of all exported symbols
#' }
#' Everything else is ignored.
#' @return string representing MD5 digest of the object in lowercase.
#' @export
metadata.digest<-function(metadata, flag_include_objectrecords)
{
  assertMetadata(metadata)

  ans<-depwalker:::calculate_task_state_digest(metadata, flag_full_hash = FALSE, flag_include_objectrecords=TRUE)

  # #Do policzenia digestu potrzeba 2 komponentów:
  # # 1) digestu z parents
  # # 2) digestu z kodu
  # # 3) digestu z objects
  #
  # parents.digest<-parents.digest(metadata)
  # code.digest<-calculate_code_digest(metadata)
  # objects.digest<-objects.digest(metadata)
  # runtime.digest<-runtime.digest(metadata)
  #
  # ans<-digest::digest(paste0(parents.digest,"><",code.digest, "><", objects.digest),serialize = FALSE)
  assertDigest(ans)

  return(ans)
}



#' Returns all files with code in canonical order:
#' first the main file,
#' then all the auxilary files sorted alphabetically including paths
get_coding_files<-function(metadata, flag_expand_paths)
{
  if (!is.null(metadata$extrasources))
  {
    extranames<-plyr::laply(metadata$extrasources,
      function(s)
        {
          if (is.null(s$flag.binary)) #missing binary flag means the file is not binary
            return(s$path)
          if (!s$flag.binary)
          {
            return(s$path)
          } else {
            return('')
          }
        })
    extranames2<-extranames[extranames!='']
    ans <- c(metadata$codepath, extranames2[order(extranames2)])
  } else {
    ans<-metadata$codepath
  }

  if (is.null(ans))
    return(NULL)

  if (flag_expand_paths)
  {
    ans <- plyr::aaply(ans, 1, function(path) pathcat::path.cat(dirname(metadata$path), path))
  }

  return(ans)

}

#Gets the list of all binary files in their cannonical order
get_binary_files<-function(metadata, flag_expand_paths)
{
  if (!is.null(metadata$extrasources))
  {
    extranames<-plyr::laply(metadata$extrasources,
                            function(s)
                            {
                              if (is.null(s$flag.binary)) #missing binary flag means the file is not binary
                                return('')
                              if (s$flag.binary)
                              {
                                return(s$path)
                              } else {
                                return('')
                              }
                            })
    extranames2<-extranames[extranames!='']
    ans<-extranames2[order(extranames2)]
  } else {
    return(list())
  }

    if (flag_expand_paths)
  {
    ans <- plyr::aaply(ans, 1, function(path) pathcat::path.cat(dirname(metadata$path), path))
  }

  return(ans)
}

#' Calculated MD5 digest of a signle source file.
#' I don't use tools::md5sum to make sure, that the digest is independent on newlines format (Windows/Linux)
calculate_one_digest<-function(code)
{
  if(is.raw(code)) {
    codedigest<-digest::digest(code, serialize=FALSE)
  } else {

  }
  if (length(code)>1)
    codedigest<-digest::digest(
      do.call(
        paste0,
        lapply(code,
               function(x) digest::digest(x,algo='md5',serialize=FALSE,ascii=TRUE))),
      algo='md5',serialize=FALSE,ascii=TRUE)
  else
    codedigest<-digest::digest(code, serialize=FALSE)
  assertDigest(codedigest)
  return(codedigest)
}

source_file_digest<-function(path)
{
  code<-readLines(path)

  return(calculate_one_digest(code))
}


#' Returns object's digest for testing equivalence of metadatas
#' Digest is solely based on names of the objects
#' @param metadata Task's metadata
objects.digest<-function(metadata)
{
  objectnames<-sapply(metadata$objectrecords, function(x) x$name)

  idx<-order(objectnames)
  single.digest<-function(idx)
  {
    o<-metadata$objectrecords[[idx]]
    return(paste0(o$name))
  }
  objects<-sapply(idx, single.digest)
  objects<-paste0(objects, collapse = ' ')
  d<-digest::digest(objects, serialize=FALSE)
  checkmate::assertString(objects)
  return(d)
}

#' Returns runtime environment's digest for testing equivalence of metadataas
#' Digest uses object's save path and object names only
#' @param metadata Task's metadata
runtime.digest<-function(metadata)
{
  if(length(metadata$inputobjects)>0) {
    df<-dplyr::arrange(
      dplyr::select(
        lists_to_df(
          metadata$inputobjects,
          list_columns = c('name', 'objectdigest', 'size')),
        name, ignored, path),
      -ignored, name)
    ans<-''
    for(i in seq(1, nrow(df))) {
      name<-paste0(df$path[[i]], collapse=',')
      if(i>1) {
        ans<-paste0(ans, '|')
      }
      if(df$ignored) {
        ans<-paste0(ans, ":", name)
      } else {
        ans<-paste0(ans, ":", df$path[[i]], "/",  name)
      }
    }
  } else {
    ans<-''
  }
  d<-digest::digest(ans, serialize=FALSE)
  checkmate::assertString(ans)
  return(d)
}

#' Returns digest of object's parent information.
#' This include:
#' 1. name of the imported object
#' 2. aliasname of the imported object as it is used by us
parents.digest<-function(metadata)
{
  if(length(metadata$parents)>0) {
    parentnames<-names(metadata$parents)


    idx<-order(parentnames)
    single.parent.digest<-function(idx)
    {
      p<-metadata$parents[[idx]]
      names<-paste0(p$name, collapse=",")
      anames<-paste0(p$aliasname, collapse=",")
      return(paste0(p$path, "|", names, "|", anames ))
    }
    parents<-sapply(idx, single.parent.digest)
    parents<-paste0(parents, collapse = ' ')
  } else {
    parents<-''
  }
  d<-digest::digest(parents,serialize=FALSE)
  return(d)
}

