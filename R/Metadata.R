#This is an object that describe the metadata

ObjectMetadata<-R6::R6Class(
  "ObjectMetadata",
  public = list(
    initialize=function(code=NULL, source.path=NULL, metadata.path, flag.never.execute.parallel=FALSE, execution.directory='') {
      private$m <- create.metadata(code=code, source.path=source.path, metadata.path=metadata.path,
                                   flag.never.execute.parallel = flag.never.execute.parallel,
                                   execution.directory = execution.directory)
    },
    #Saves the metadata
    save = function() {
      make.sure.metadata.is.saved(private$m)
      NULL
    },
    add_parent=function(parent=NULL, name=NULL, aliasname=NULL, flag_remember_absolute_path=FALSE) {
      if('character' %in% class(parent)) {
        parent.path<-parent
        parent<-NULL
      } else {
        parent.path<-NULL
      }

      if(!is.null(parent)) {
        if('ObjectMetadata' %in% class(parent) && 'R6' %in% class(parent)){
          parent<-parent$.__enclos_env__$private$m
        }
      }

      m<-add.parent(metadata = private$m, parent.path = parent.path, parent = parent, name=name,
                    aliasname = aliasname, flag_remember_absolute_path=flag_remember_absolute_path)
      private$m<-m
    },
    add_objectrecord=function(name, path=NULL, compress='xz') {
      m<-add.objectrecord(metadata = private$m, name = name, path = path, compress = compress)
      private$m<-m
    },
    add_source_file=function(code=NULL, source.path, flag.binary=FALSE, flag.checksum=TRUE) {
      m<-add_source_file(metadata = private$m,code = code, filepath = source.path, flag.binary = flag.binary, flag.checksum = flag.checksum )
      private$m<-m
    },
    #True oznacza, że się udało
    load_objects=function(target.environment=NULL, objectnames=NULL,
                          flag.save.intermediate.objects=TRUE,
                          flag.allow.promises=TRUE,
                          flag.check.md5sum=TRUE, flag.save.in.background=TRUE,
                          flag.check.object.digest=TRUE, flag.ignore.mtime=FALSE) {
      if(is.null(target.environment)) {
        stop("target.environment is an obligatory argument. Perhaps you want to use get_objects()?")
      }

      ans<-load.object(metadata=private$m, objectnames=objectnames,
                       target.environment=target.environment,
                       flag.save.intermediate.objects=flag.save.intermediate.objects,
                       flag.check.md5sum=flag.check.md5sum,
                       flag.save.in.background=flag.save.in.background,
                       flag.check.object.digest=flag.check.object.digest,
                       flag.ignore.mtime=flag.ignore.mtime,
                       flag.allow.promises=TRUE)
      return(ans)
    },
    get_objects=function(objectnames=NULL,
                         flag.save.intermediate.objects=TRUE,
                         flag.check.md5sum=TRUE, flag.save.in.background=TRUE,
                         flag.check.object.digest=TRUE, flag.ignore.mtime=FALSE,
                         flag.return.list=FALSE) {
      ans<-get.object(metadata=private$m, objectnames=objectnames,
                      flag.save.intermediate.objects=flag.save.intermediate.objects,
                      flag.check.md5sum=flag.check.md5sum,
                      flag.save.in.background=flag.save.in.background,
                      flag.check.object.digest=flag.check.object.digest,
                      flag.ignore.mtime=flag.ignore.mtime)
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
