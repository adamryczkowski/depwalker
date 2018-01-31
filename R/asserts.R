assertMetadata<-function(metadata, flag_ready_to_run=TRUE)
{
  valid_items<-c('path',
                 'inputfiles', 'inputobjects_storage', 'inputobjects',
                 'parents', 'objectrecords_storage',  'objectrecords',
                 'runtime_environment', 'execution_directory',
                 'flag_never_execute_parallel',
                 'flag_use_tmp_storage', 'flag_force_recalculation', 'timecosts')
  if(length(setdiff(names(metadata), valid_items))>0) {
    stop(paste0("Unkown components of the metadata: ",
                paste0(setdiff(names(metadata), valid_items)), collapse=', '))
  }

  if(flag_ready_to_run) {
    checkmate::assertPathForOutput(metadata$path, overwrite=TRUE)
  } else {
    assertVariableName(basename(metadata$path))
  }

  checkmate::assertList(metadata$inputfiles)
  for (inputfile in metadata$inputfiles)
  {
    assertInputFilesMetadata(inputfile, metadata)
  }
  if(flag_ready_to_run) {
    checkmate::assert_true(length(metadata$inputfiles)>0)
    df<-objectstorage::lists_to_df(metadata$inputfiles)
    checkmate::assert_true(sum(df$type=='RMain')==1) #There must be exactly one object of type RMain.
  }

  assertValidPath(metadata$inputobjects_storage)
  checkmate::assertList(metadata$inputobjects)
  for(inputobject in metadata$inputobjects) {
    assertInputObjectMetadata(inputobject, metadata)
  }

  checkmate::assertList(metadata$parents)
  for (parentrecord in metadata$parents)
  {
    assertParentRecordMetadata(parentrecord, metadata)
  }

  df<-get_runtime_objects(metadata, TRUE, TRUE)
  if(sum(duplicated(df$objectname))>0) {
    stop(paste0("Objects ", paste0(df$objectname[duplicated(df$objectname)], collapse=', '),
                " will be duplicated in the task's runtime environemnt."))
  }

  assertValidPath(metadata$objectrecords_storage)
  checkmate::assertList(metadata$objectrecords)
  for (objectrecord in metadata$objectrecords)
  {
    assertObjectRecordMetadata(objectrecord, metadata)
  }

  checkmate::assert_environment(metadata$runtime_environment)

  path<-get_path(metadata, metadata$execution_directory)
  assertValidPath(path)

  checkmate::assertFlag(metadata$flag_never_execute_parallel)
  checkmate::assertFlag(metadata$flag_use_tmp_storage)
  checkmate::assertFlag(metadata$flag_force_recalculation)



  checkmate::assertDataFrame(metadata$timecosts)
  cols<-c('walltime', 'cputime', 'systemtime','cpumodel', 'membefore',
          'memafter', 'corecount', 'virtualcorecount', 'busycpus')
  if (!all(cols %in% colnames(metadata$timecosts)))
    stop("Insufficient columns in metadata$timecosts data.frame")

}

assertInputFilesMetadata<-function(inputfiles, metadata)
{
  valid_items<-c('path', 'flag_checksum', 'type', 'digest')

  if(length(setdiff(names(inputfiles), valid_items))>0) {
    stop(paste0("Unkown components of the inputfiles: ",
                paste0(setdiff(names(inputfiles), valid_items)), collapse=', '))
  }
  assertValidPath(inputfiles$path)

  checkmate::assert_flag(inputfiles$flag_checksum)

  assertSourceType(inputfiles$type)

  assertDigest(inputfiles$digest)
}

assertInputObjectMetadata<-function(inputrecord, metadata)
{
  valid_items<-c('name', 'ignored', 'digest')

  if(length(setdiff(names(inputrecord), valid_items))>0) {
    stop(paste0("Unkown components of the inputrecord: ",
                paste0(setdiff(names(inputrecord), valid_items)), collapse=', '))
  }

  assertVariableName(inputrecord$name)
  checkmate::assert_logical(inputrecord$ignored)
  assertDigest(inputrecord$digest, flag_allow_empty = TRUE)

}

assertParentRecordMetadata<-function(parentrecord, metadata)
{
  assertVariableNames(parentrecord$names)
  assertVariableNames(parentrecord$aliasnames)
  path=get_path(metadata, parentrecord$path, extension='metadata')
  assertValidPath(path)
  if (!file.exists(path))
    stop(paste0("Parent tranformation ", parentrecord$name, " cannot be found in ",path))

  assertDigest(parentrecord$digest, flag_allow_empty=TRUE)
}

assertObjectRecordMetadata<-function(objectrecord, metadata)
{
  valid_items<-c('name', 'archivepath', 'compress', 'digest')

  if(length(setdiff(names(objectrecord), valid_items))>0) {
    stop(paste0("Unkown components of the objectrecord: ",
                paste0(setdiff(names(objectrecord), valid_items)), collapse=', '))
  }

  assertVariableName(objectrecord$name)
  path=get_path(metadata, objectrecord$path)
  assertValidPath(path)
  assertCompress(objectrecord$compress)
  assertDigest(objectrecord$digest, flag_allow_empty = TRUE)
}


assertMtime<-function(mtime)
{
  #  assertString(mtime)
  as.POSIXct(mtime)
}


assertDigest<-function(digest, flag_allow_empty=FALSE)
{
  if(is.na(digest)) {
    if(!flag_allow_empty) {
      stop("Empty digest not allowed!")
    }
  }
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

assertCompress<-function(compress) {
  checkmate::assertChoice(compress, c('xz','gzip','bzip2','none'))
}

assertSourceType<-function(type) {
  checkmate::assertChoice(inputfiles$type, c('R', 'RMain', 'txt', 'binary'))
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

assertDigests<-function(digests)
{
  for(digest in digests) {
    assertDigest(digest)
  }
}



assertInt64<-function(var)
{
  if (!bit64::is.integer64(var))
    stop(paste0("Variable ",var," should be of class integer64!"))
}


assertTimeEstimation<-function(te)
{
  checkmate::assertList(te)
}
