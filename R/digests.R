#' Returns digest of the task's metadata.
#'
#' Digests are used in many places for easy identification of equivalent tasks' metadatas.
#'
#' The digest is calculated on the task's essential properties:
#' \itemize{
#'  \item \strong{parent records}.
#'    \describe{
#'      \item{\code{path}}{path to the ancestor's task metadata}
#'      \item{\code{name}}{name of the R object calculated by the ancestor}
#'      \item{\code{aliasname}}{optional. Specifies alternate name of the object for our R script}
#'    }
#'  \item \strong{code}.
#'    All source code used by our task
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
#'
#' @return string representing MD5 digest of the object in lowercase.
#' @export
metadata.digest<-function(metadata)
{
  assertMetadata(metadata)
  #Do policzenia digestu potrzeba 2 komponentów:
  # 1) digestu z parents
  # 2) digestu z kodu
  # 3) digestu z objects

  parents.digest<-parents.digest(metadata)
  code.digest<-calculate_code_digest(metadata)
  objects.digest<-objects.digest(metadata)

  ans<-digest::digest(paste0(parents.digest,"><",code.digest, "><", objects.digest),serialize = FALSE)
  assertDigest(ans)
  return(ans)
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


#' Returns object's digest for testing equivalence of metadataas
#' Digest is solely based on names of the objects:
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

#' Returns digest of object's parent information.
#' This include:
#' 1. name of the imported object
#' 2. aliasname of the imported object as it is used by us
parents.digest<-function(metadata)
{
  parentnames<-sapply(metadata$parents, function(x) {if(is.null(x$aliasname)) {x$name;} else {x$aliasname;}})

  idx<-order(parentnames)
  single.parent.digest<-function(idx)
  {
    p<-metadata$parents[[idx]]
    if (is.null(p$aliasname))
      return(paste0(p$path, "|", p$name))
    else
      return(paste0(p$path, "|", p$name,"|",p$aliasname))
  }
  parents<-sapply(idx, single.parent.digest)
  parents<-paste0(parents, collapse = ' ')
  d<-digest::digest(parents,serialize=FALSE)
  return(d)
}

#' Calculates object's digest
#'
#' It efficiently calculates object's digest.
#'
#' It treats \code{data.table} objects separately. Rather than invoking \code{digest::digest()}
#' on them directly, it first splits the object into individual columns, sorts them,
#' calculates the
#' digest on each column separately (trying to do that in parallel), and merges the
#' results into one string, that is digested again.
#'
#' @param object The name of the object to calculate digest of. Object must exist in
#'   \code{.GlobalEnv}.
#'
#' @return Lowercase MD5 string with the digest of the object
#'
#'
#Funkcja kalkuluje object.digest obiektu. Nie wkłada go do parentrecord.
#Dla obiektów typu data.frame używany jest szczególnie wydajny pamięciowo
#algorytm, który liczy digest zmienna-po-zmiennej
calculate.object.digest<-function(object)
{
  if (!is.character(object))
    stop('Needs string parameter')

  #Należy usunąć nasze metadane do kalkulacji digestu, bo metadane same mogą zawierać digest i nigdy nie uzyskamy spójnych wyników
  parentrecord<-attr(get(object, envir=.GlobalEnv),'parentrecord')
  if (!is.null(parentrecord))
    eval(parse(text=paste0('setattr(', object, ", 'parentrecord', NULL)")),envir=.GlobalEnv)

  if (data.table::is.data.table(get(object, envir=.GlobalEnv)))
  {
    d<-tryCatch(parallel::mclapply(get(object,envir = .GlobalEnv) , function(x) digest::digest(x, algo="md5")),
      error=function(e) e)
    if ('error' %in% class(d))
    {
      d<-lapply(get(object,envir = .GlobalEnv) , function(x) digest::digest(x, algo="md5"))
    }
    d<-digest::digest(d[order(names(d))])
  } else {
    d<-digest::digest(get(object, envir=.GlobalEnv))
  }
  assertDigest(d)
  return(d)
}
