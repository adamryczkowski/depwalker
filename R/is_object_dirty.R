

#' Returns TRUE if code has been changed by external force, forcing us to do
#' recalculation.
code_has_been_changed<-function(metadata)
{
  if (!is.null(metadata$codeCRC))
  {
    digests<-calculate_code_digest(metadata)
    return(metadata$codeCRC != digests)
  } else {
    return(NA)
  }
}

