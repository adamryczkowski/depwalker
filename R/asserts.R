assertMtime<-function(mtime)
{
  #  assertString(mtime)
  as.POSIXct(mtime)
}


assertDigest<-function(digest)
{
  checkmate::assertString(digest, pattern = '^[0-9a-f]{32}$')
}

assertVariableName<-function(varname)
{
  checkmate::assertString(varname, pattern="^([[:alpha:]]|[.][._[:alpha:]])[._[:alnum:]]*$")
}

assertVariableNames<-function(varnames)
{
  for (varname in varnames)
    checkmate::assertString(varname, pattern="^([[:alpha:]]|[.][._[:alpha:]])[._[:alnum:]]*$")
}

assertCode<-function(code)
{
  checkmate::assertCharacter(code)
  out<-tryCatch({parse(text=code);NULL}, error = function(x){x})
  if (!is.null(out))
  {
    return(out$message)
  }
  else
  {
    return('')
  }
}

assertMetadata<-function(metadata)
{
  checkmate::assertList(metadata$objectrecords)
  checkmate::assertPathForOutput(metadata$path, overwrite=TRUE)
  if (!is.null(metadata$codepath))
    checkmate::assert_file_exists(get.codepath(metadata))
  a<-assertCode(metadata$code)
  if (a!='')
    stop(paste0('Invalid R code: ',a))
  objectnames<-sapply(metadata$objectrecords, function(x)x$name)
  checkmate::assert(length(objectnames)==length(unique(objectnames)), .var.name="Non-unique object names" )
  for (objectrecord in metadata$objectrecords)
  {
    assertObjectRecordMetadata(objectrecord, metadata)
  }
  for (parentrecord in metadata$parents)
  {
    assertParentRecordMetadata(parentrecord, metadata)
  }

  if (!is.null(metadata$execution.directory))
  {
    path<-get.codepath(metadata, metadata$execution.directory)
    assertValidPath(path)
  }

  checkmate::assertList(metadata$parents)
  if (!is.null(metadata$flag.never.execute.parallel))
    checkmate::assertFlag(metadata$flag.never.execute.parallel)
  if (!is.null(metadata$flag.update.cost.statistics))
    checkmate::assertFlag(metadata$flag.update.cost.statistics)
  if (!is.null(metadata$flag.force.recalculation))
    checkmate::assertFlag(metadata$flag.force.recalculation)
  if (!is.null(metadata$timecosts))
  {
    checkmate::assertDataFrame(metadata$timecosts)
    cols<-c('walltime', 'cputime', 'systemtime','cpumodel', 'membefore', 'memafter', 'corecount', 'virtualcorecount', 'busycpus')
    if (!all(cols %in% colnames(metadata$timecosts)))
      stop("Insufficient columns in metadata$timecosts data.frame")
  }
  if (!is.null(metadata$extrasources))
  {
    for (extrasource in metadata$extrasources)
    {
      checkmate::assert_file_exists(get.codepath(metadata, extrasource$path))
      checkmate::assert_flag(extrasource$flag.checksum)
      checkmate::assert_flag(extrasource$flag.binary)
      checkmate::assert_flag(extrasource$flag.r)
    }
  }
  if (!is.null(metadata$codeCRC))
    assertDigest(metadata$codeCRC)
}

assertValidPath<-function(path)
{
  checkmate::assertString(path, pattern= "[^\\0]+")
}

assertFileExists<-function(path)
{
  checkmate::assert_file_exists(path)
}

assertDigest<-function(digest)
{
  checkmate::assertString(digest, pattern = '^[0-9a-f]{32}$')
}

assertParentRecordMetadata<-function(parentrecord, metadata)
{
  assertVariableNames(parentrecord$name)
  assertVariableNames(parentrecord$aliasname)
  path=get.parentpath(parentrecord, metadata)
  assertValidPath(path)
  if (!is.null(parentrecord$digest))
    assertDigest(parentrecord$digest)
  if (!file.exists(path))
    stop(paste0("Parent tranformation ", parentrecord$name, " cannot be found in ",path))
}

assertInt64<-function(var)
{
  if (!bit64::is.integer64(var))
    stop(paste0("Variable ",var," should be of class integer64!"))
}

assertObjectRecordMetadata<-function(objectrecord, metadata)
{
  assertVariableName(objectrecord$name)
  path=get.objectpath(objectrecord, metadata)
  assertValidPath(path)
  checkmate::assertChoice(objectrecord$compress, c('xz','gzip','bzip2',FALSE))
  if (!is.null(objectrecord$mtime))
  {
    assertMtime(objectrecord$mtime)
    if (!is.null(objectrecord$filedigest))
      assertDigest(objectrecord$filedigest)
    assertInt64(objectrecord$filesize)
  }
  if (!is.null(objectrecord$size))
    assertInt64(objectrecord$size)
  if (!is.null(objectrecord$objectdigest))
    assertDigest(objectrecord$objectdigest)
}

assertTimeEstimation<-function(te)
{
  checkmate::assertList(te)
}
