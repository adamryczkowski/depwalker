#This is an object that describe the metadata. It doesn't store the data, it only do that when the task is not saved.
#Once the task is saved, it points to the disk.

ObjectMetadata<-R6::R6Class(
  "ObjectMetadata",
  public = list(
    initialize=function(code=NULL, source_path=NULL, metadata_path, flag_never_execute_parallel=FALSE, execution_directory='') {
      private$m <- create_metadata(code=code, source_path=source_path, metadata_path=metadata_path,
                                   flag_never_execute_parallel = flag_never_execute_parallel,
                                   execution_directory = execution_directory)
    },
    #Saves the metadata
    save = function() {
      make_sure_metadata_is_saved(private$m)
      NULL
    },
    add_parent=function(parent=NULL, name=NULL, aliasname=NULL, flag_remember_absolute_path=FALSE) {
      if('character' %in% class(parent)) {
        parent_path<-parent
        parent<-NULL
      } else {
        parent_path<-NULL
      }

      if(!is.null(parent)) {
        if('ObjectMetadata' %in% class(parent) && 'R6' %in% class(parent)){
          parent<-parent$.__enclos_env__$private$m
        }
      }

      m<-add_parent(metadata = private$m, parent_path = parent_path, parent = parent, name=name,
                    aliasname = aliasname, flag_remember_absolute_path=flag_remember_absolute_path)
      private$m<-m
    },
    add_objectrecord=function(name, path=NULL, compress='xz') {
      m<-add_objectrecord(metadata = private$m, name = name, path = path, compress = compress)
      private$m<-m
    },
    add_source_file=function(code=NULL, source_path, flag_binary=FALSE, flag_checksum=TRUE) {
      m<-add_source_file(metadata = private$m,code = code, filepath = source_path, flag_binary = flag_binary, flag_checksum = flag_checksum )
      private$m<-m
    },
    #True oznacza, że się udało
    load_objects=function(target_environment=NULL, objectnames=NULL,
                          flag_save_intermediate_objects=TRUE,
                          flag_allow_promises=TRUE,
                          flag_check_md5sum=TRUE, flag_save_in_background=TRUE,
                          flag_check_object_digest=TRUE, flag_ignore_mtime=FALSE) {
      if(is.null(target_environment)) {
        stop("target.environment is an obligatory argument. Perhaps you want to use get_objects()?")
      }

      ans<-load_object(metadata=private$m, objectnames=objectnames,
                       target_environment=target_environment,
                       flag_save_intermediate_objects=flag_save_intermediate_objects,
                       flag_check_md5sum=flag_check_md5sum,
                       flag_save_in_background=flag_save_in_background,
                       flag_check_object_digest=flag_check_object_digest,
                       flag_allow_promises=TRUE)
      return(ans)
    },
    get_objects=function(objectnames=NULL,
                         flag_save_intermediate_objects=TRUE,
                         flag_check_md5sum=TRUE, flag_save_in_background=TRUE,
                         flag_check_object_digest=TRUE,
                         flag_return_list=FALSE) {
      ans<-get_object(metadata=private$m, objectnames=objectnames,
                      flag_save_intermediate_objects=flag_save_intermediate_objects,
                      flag_check_md5sum=flag_check_md5sum,
                      flag_save_in_background=flag_save_in_background,
                      flag_check_object_digest=flag_check_object_digest)
      return(ans)
    },
    print=function() {
      cat(depwalker:::print_m(private$m))
    },
    dump_data=function() {
      metadata.dump(private$m$path)
    }
  ),
  active = list(
    path = function() {private$m$path}
  ),
  private = list(
    m = NA #metadata object to manipilate
  ),
  lock_class = TRUE,
  lock_objects = TRUE,
  cloneable = FALSE
  )
