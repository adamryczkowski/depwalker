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
  code.digest<-calculate.code.digest(metadata)
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
calculate.code.digest<-function(metadata)
{
  if (length(metadata$code)>1)
    codedigest<-digest::digest(
      do.call(
        paste0,
        lapply(metadata$code,
               function(x) digest::digest(x,algo='md5',serialize=FALSE,ascii=TRUE))),
      algo='md5',serialize=FALSE,ascii=TRUE)
  else
    codedigest<-digest::digest(metadata$code, serialize=FALSE)
  assertDigest(codedigest)
  return(codedigest)
}

#' Returns object's digest for testing equivalence of metadataas
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
    d<-tryCatch(
      eval(parse(text=paste0(object,'[,parallel::mclapply(.SD,function(x){gc();digest::digest(x,algo="md5")})]')),envir=.GlobalEnv),
      error=function(e) e)
    if ('error' %in% class(d))
    {
      d<-eval(parse(text=paste0(object,'[,lapply(.SD,function(x){gc();digest::digest(x,algo="md5")})]')),envir=.GlobalEnv)
    }
    d<-digest::digest(d[order(names(d))])
  } else {
    d<-digest::digest(get(object, envir=.GlobalEnv))
  }
  assertDigest(d)
  return(d)
}
