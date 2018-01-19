
# Returns string that characterises CPU.
cpuinfo<-function()
{
  return(sfsmisc::Sys.cpuinfo()[5])
}

# Returns number of CPUs
cpu.count<-function()
{
  a<-system('lscpu -p', intern=TRUE)
  a<-a[4:length(a)]
  con <- textConnection(a)
  data <- read.csv(con)
  close(con)
  return(list(core.count=length(unique(data$Core)), virtual.core.count=nrow(data)))
}

# Returns table with usage statistics of all processors
cpu.usage<-function()
{
  ans<-system('mpstat -P ON 1 1', intern=TRUE)
  a<-ans[3:length(ans)]
  pos<-which(a=='')
  a<-a[c(1, 3:(pos-1))]
  data <- data.table::fread(paste0(a,collapse='\n'))
  data[,colnames(data)[1]:=NULL]
  #  print(data)
  return(data.table::as.data.table(data))
}

# Returns list of processors, which usage is over 75%
cpu.usage.list<-function()
{
  data<-cpu.usage()
  n<-'%usr'
  busy.cpus<-sum(data[,n,with=FALSE]>75)
  return(list(busy.cpus=busy.cpus))
}

# Returns full path to the code
#' @export
get.codepath<-function(metadata, path=NULL)
{
  if (is.null(path))
  {
    path <- metadata$codepath
  }
  path <- pathcat::path.cat(getwd(), dirname(metadata$path), path)
  return(path)
}

get.parentpath<-function(parentrecord, metadata=NULL, metadata.path=NULL, flag_include_extension=TRUE)
{
  if (is.null(metadata) & is.null(metadata.path))
    stop("Unkown metadata.path")
  if (is.null(metadata.path))
    metadata.path<-metadata$path
  ans<-pathcat::path.cat(dirname(metadata.path), parentrecord$path)
  if (flag_include_extension)
    ans<-paste0(ans,getOption('metadata.save.extension'))
  return(ans)
}

get.objectpath<-function(objectrecord, metadata=NULL, metadata.path=NULL)
{
  if (is.null(metadata) & is.null(metadata.path))
    stop("Unkown metadata.path")
  if (is.null(metadata.path))
    metadata.path<-metadata$path
  return(pathcat::path.cat(dirname(metadata.path), objectrecord$path))
}

#Gives a full path made from relative path and the metadata path
get.fullpath<-function(metadata, path)
{
  return(pathcat::path.cat(dirname(metadata.path), path))
}


#' Returns specific subset of objectrecords
#' @param metadata Task's metadata
#' @param objnames Character vector with names of objects for which we want to
#'   get objectrecords
get.objectrecords<-function(metadata, objnames)
{
  objectnames<-sapply(metadata$objectrecords, function(x)x$name)
  idx<-which(objectnames %in% objnames )
  return(metadata$objectrecords[idx])
}

#' Tests whether two task metadata's define the same task equal (not necessarily identical)
#'
#' @param m1 First metadata to compare
#' @param m2 Second metadata to compare
#' @return \code{TRUE} if identical, \code{FALSE} otherwise
#' @export
are.two.metadatas.equal<-function(m1, m2)
{
  return(metadata.digest(m1)==metadata.digest(m2))
}

#Add some extra information present in extra_m into the base_m, retaining the
#log and statistics of the base_m, and any other automatically-added info, like CRC or mtimes.
join.metadatas<-function(base_m, extra_m)
{
  #Two metadatas have the same digest - i.e. they agree on:
  # 1. names and aliasnames of all parents
  # 2. code
  # 3. names of all exported symbols

  dirty=FALSE

  fnupdate<-function(name, base_m, extra_m)
  {
    if (!is.null(extra_m[[name]]))
    {
      if (!is.null(base_m[[name]]))
      {
        if (extra_m[[name]] != base_m[[name]])
        {
          base_m[[name]]<<-extra_m[[name]]
          return(TRUE)
        }
      } else {
        base_m[[name]]<<-extra_m[[name]]
        return(TRUE)
      }
    }
    return(FALSE)
  }


  dirty<-fnupdate('codepath', base_m, extra_m)
  dirty<-dirty || fnupdate('flag.never.execute.parallel', base_m, extra_m)
  dirty<-dirty || fnupdate('flag.force.recalculation', base_m, extra_m)
  dirty<-dirty || fnupdate('execution.directory', base_m, extra_m)


  join_objectrecords<-function(base_o, extra_o)
  {
    dirty<-FALSE
    dirty <- dirty || fnupdate('path', base_o, extra_o)
    dirty <- dirty || fnupdate('compress', base_o, extra_o)
    return(dirty)
  }
  for (i in seq_along(base_m$objectrecords)) #We can assume that number of objectrecords match,
    #otherwise two metadatas would not be equivalent
  {
    base_o <- base_m$objectrecords[[i]]
    extra_o <- extra_m$objectrecords[[i]]
    if (join_objectrecords(base_o, extra_o))
    {
      base_m$objectrecords[[i]]<-extra_o
      dirty<-TRUE
    }
  }


  join_extrasources<-function(base_o, extra_o)
  {
    dirty<-FALSE
    dirty <- dirty || fnupdate('flag.checksum', base_o, extra_o)
    dirty <- dirty || fnupdate('flag.r', base_o, extra_o)
    return(dirty)
  }
  for (i in seq_along(base_m$extrasources)) #We can assume that number of extrasources match,
    #otherwise two metadatas would not be equivalent
  {
    base_o <- base_m$extrasources[[i]]
    extra_o <- extra_m$extrasources[[i]]
    if (join_extrasources(base_o, extra_o))
    {
      base_m$extrasources[[i]]<-extra_o
      dirty<-TRUE
    }
  }


  join_inputobjects<-function(base_o, extra_o)
  {
    dirty<-FALSE
    dirty <- dirty || fnupdate('compress', base_o, extra_o)
    return(dirty)
  }
  for (i in seq_along(base_m$inputobjects)) #We can assume that number of extrasources match,
    #otherwise two metadatas would not be equivalent
  {
    base_o <- base_m$inputobjects[[i]]
    extra_o <- extra_m$inputobjects[[i]]
    if (join_inputobjects(base_o, extra_o))
    {
      base_m$inputobjects[[i]]<-extra_o
      dirty<-TRUE
    }
  }

  if (dirty)
  {
    return(base_m)
  } else {
    return(NULL)
  }
}

get.objectrecord.by.parentrecord<-function(parentrecord, metadata)
{
  assertParentRecordMetadata(parentrecord,metadata)

  #1. Wczytujemy metadane
  #2. Odszukujemy objectrecord
  #3. Zwracamy go.
  m<-load.metadata.by.parentrecord(metadata, parentrecord)
  if (is.null(m))
    return(NULL)
  or<-get.objectrecords(m, parentrecord$name)
  if(length(or)!=1)
    return(NULL)
  return(or[[1]])
}

#' Returns sorted parentrecords.
#'
#'  @param metadata Metadata object
#'  @return  Sorted lists of items. Each item corresponds to one imported task and is a list of elements:
#'  \describe{
#'  \item{\code{path}}{path to the metadata}
#'  \item{\code{names}}{character vector with names of exported objects from this metadata}
#'  \item{\code{aliasnames}}{character vector of the names used by our script}
#'  }
sort.parentrecords<-function(metadata)
{
  ans<-list()
  for(p in metadata$parents)
  {
    if (is.null(ans[p$path]))
    {
      ans[[p$path]]<-list(path=p$path, names=p$name, aliasnames=p$aliasname)
    } else
    {
      ans[[p$path]]$names<-c(ans[p$path]$names, p$name)
      ans[[p$path]]$aliasnames<-c(ans[p$path]$aliasnames, p$aliasname)
      ans[[p$path]]$path<-p$path
    }
  }
  return(ans)
}

#' Returns total size of all objects or NA
#' @param metadata Task's metadata
metadata.objects.size<-function(metadata)
{
  suma<-bit64::as.integer64(0)
  for(objectrecord in metadata$objectrecords)
  {
    if (is.null(objectrecord$size))
    {
      return(NA)
    }
    suma<-suma+objectrecord$size
  }
  return(suma)
}



# memfree<-function()
# {
#   tryCatch(
#     as.numeric(system("awk '/MemAvailable/ {print $2}' /proc/meminfo", intern=TRUE)),
#     error = function(e) 0)
# }

#' Returns free memory in kB. This function is strictly Linux-specific.
memfree<-function()
{
  as.numeric(gsub(
    pattern='^([0-9]+)\\s.*$',
    x=sfsmisc::Sys.meminfo()['MemFree'][[1]],
    replacement='\\1'))
}

mytraceback<-function (x = NULL, max.lines = getOption("deparse.max.lines"))
{
  if (is.null(x) && !is.null(x <- get0(".Traceback", envir = baseenv()))) {
  }
  else if (is.numeric(x))
    x <- .traceback(x)
  n <- length(x)
  if (n == 0L)
    cat(gettext("No traceback available"), "\n")
  else {
    for (i in 1L:n) {
      xi <- x[[i]]
      if (class(xi)=='call')
        xi<-deparse(xi)
      label <- paste0(n - i + 1L, ": ")
      m <- length(xi)
      srcloc <- if (!is.null(srcref <- attr(xi, "srcref"))) {
        srcfile <- attr(srcref, "srcfile")
        paste0(" at ", basename(srcfile$filename), "#",
               srcref[1L])
      }
      if (is.numeric(max.lines) && max.lines > 0L && max.lines <
          m) {
        xi <- c(xi[seq_len(max.lines)], " ...")
        m <- length(xi)
      }
      if (!is.null(srcloc)) {
        xi[m] <- paste0(xi[m], srcloc)
      }
      if (m > 1)
        label <- c(label, rep(substr("          ", 1L,
                                     nchar(label, type = "w")), m - 1L))
      cat(paste0(label, xi), sep = "\n")
    }
  }
  # invisible(x)
}

normalize_code_string<-function(code)
{
  code[code=='']<-'\n' #Otherwise empty rows will be removed from the string
  code<-unlist(strsplit(code,'\n')) #Makes sure each line is in separate element
  return(code)
}

lists_to_df<-function(l, list_columns=character(0)) {
  cns<-names(l[[1]])
  nrow<-length(l)

  dt<-data.table(..delete=rep(NA, nrow))
  for(cn in cns) {
    if(cn %in% list_columns) {
      val<-list(list())
    } else {
      val<-l[[1]][[cn]]
      val[[1]]<-NA
    }
    dt[[cn]]<-rep(val, nrow)
  }
  for(i in seq(1, nrow)) {
    for(cn in cns) {
      val<-l[[i]][[cn]]
      if(cn %in% list_columns) {
        val<-list(list(val))
      }
      set(dt, i, cn, val)
    }
  }
  dt[,..delete:=NULL]
  return(dt)
}

