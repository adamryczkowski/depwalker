

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

