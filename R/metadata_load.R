#' Loads the task's metadata object from disk
#'
#' @param metadata.path Full path to the metadata.
#'
#' @return metadata object.
#'
#' @export
load_metadata<-function(metadata.path)
{
  metadata.path<-pathcat::file_path_sans_all_ext(metadata.path)
  if (!file.exists(paste0(metadata.path,getOption('depwalker.metadata_save_extension'))))
    stop(paste0("Metadata file ",paste0(metadata.path,getOption('depwalker.metadata_save_extension'))," doesn't exists."))

  m<-yaml::yaml.load_file(paste0(metadata.path,getOption('depwalker.metadata_save_extension')))
  m$path<-metadata.path

  if(m$flag_include_global_env) {
    runtime_environment<-new.env(parent = globalenv())
  } else {
    runtime_environment<-new.env(parent = baseenv())
  }
  m$runtime_environment<-runtime_environment


  assertMetadata(m)
  return(m)
}

# Simple wrapper, that reads metadata based on the parentrecord
load_metadata_by_parentrecord<-function(metadata, parentrecord)
{
  load_metadata(parentrecord$path)
}

