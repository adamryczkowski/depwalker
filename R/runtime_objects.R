#File contain all the procedures involved in dealing with runtime_objects
#
#Index file contains a data frame with the following columns:
#objectname - name of the object contained
#digest - digest of it
#size - size in bytes in memory
#archive_filename - path to the archive
#single_object - flag if the object is a single object in the archive (and not a part of the nested list)
#

#Lists all the runtime objects cotained in the task with path path
list_runtime_objects<-function(taskpath) {
  path<-get_runtime_index_path(taskpath)
  if(file.exists(path)) {
    idx<-loadRDS(path)
    return(idx)
  } else {
    return(NULL)
  }
}

update_runtime_objects_index<-function(taskpath, newidx) {
  path<-get_runtime_index_path(taskpath)
  if(file.exists(path)) {
    unlink(path)
  }
  saveRDS(newidx, path)
}

make_sure_index_file_exists<-function(taskpath) {
  idx<-list_runtime_objects(taskpath)
  if(is.null(idx)) {
    idx<-data.frame(objectname=character(0), digest=character(0),
                    size=numeric(0), archive_filename=character(0),
                    single_object=logical(0))
    path<-get_runtime_index_path(taskpath)
    saveRDS(object = idx, file = path)
  }
  return(idx)
}


modify_runtime_archive<-function(obj.environment, addobjectnames=NULL,
                                 removeobjectnames=character(0),
                                 archive_filename, compress='gzip', wait_for='save',
                                 flag_use_tmp_storage=FALSE, parallel_cpus=NULL,
                                 tasktpath) {
  if(is.null(objectnames)){
    objectnames<-names(obj.environment)
  }

  if(length(objectnames)==0 && length(removeobjectnames)==0) {
    return() #nothing to do
  }
  archivepath<-pathcat::path.cat(dirname(tasktpath), archive_filename)

  if(length(intersect(removeobjectnames, addobjectnames))>0) {
    browser()
    #Makes no sense in adding and deleting the same object in one step
  }

  idx<-dplyr::filter(list_runtime_objects(taskpath), archive_filename==archive_filename)
  objs_to_leave<-setdiff(idx$objectnames, c(addobjectnames, removeobjectnames))
  objs_to_add<-c(addobjectnames,objs_to_leave)
  if(length(objs_to_leave)>0) {
    obj.environment<-new.env(parent = obj.environment)
    oldobjs<-readRDS(archivepath)
    for(i in seq_along(objs_to_leave)) {
      obj_to_leave<-objs_to_leave[[i]]
      rowpos<-which(idx$objectnames==obj_to_leave)
      if(idx$single_object[[rowpos]]) {
        assign(obj_to_leave,  value = oldobjs, envir = obj.environment)
      } else {
        assign(obj_to_leave,  value = oldobjs[[obj_to_leave]], envir = obj.environment)
      }
    }
  }

  if(length(objs_to_add)>1) {
    obj<-as.list(obj.environment[objs_to_add])
  } else {
    obj<-obj.environment[[objs_to_add]]
  }
  return(set_runtime_archive(obj.environment=obj.environment,
                             objectnames=objs_to_add,
                             archive_filename=archive_filename,
                             compress=compress,
                             wait_for=wait_for,
                             flag_use_tmp_storage=flag_use_tmp_storage,
                             parallel_cpus=parallel_cpus,
                             tasktpath=tasktpath))
}

set_runtime_archive<-function(obj.environment, objectnames=NULL,
                               archive_filename, compress='gzip', wait_for='save',
                               flag_use_tmp_storage=FALSE, parallel_cpus=NULL,
                               tasktpath) {
  if(is.null(objectnames)){
    objectnames<-names(obj.environment)
  }

  archivepath<-pathcat::path.cat(dirname(tasktpath), archive_filename)
  hashattrname<-getOption('reserved_attr_for_hash')
  get_digest<-function(objectname, env) {
    if(is.null(attr(get(x=objectname, envir = env),hashattrname, exact = TRUE))) {
      hash<-depwalker:::calculate.object.digest(objectname, target.environment)
    } else {
      hash<-attr(get(x=objectname, envir = env),hashattrname, exact = TRUE)
      setattr(get(x=objectname, envir = env), hashattrname, NULL)
    }
    return(hash)
  }
  digests<-purrr::map_chr(objectnames, get_digest, env=obj.environment)
  sizes<-as.numeric(purrr::map_chr(objectnames, object.size, env=obj.environment))



  if(length(objectnames)==0) {
    if(file.exists(archivepath)) {
      unlink(archivepath)
      dbchunk<-data.table(objectnames=character(0), digest=character(0), size=numeric(0),
                          single_object=logical(0),
                          archive_filename=character(0))
      return(list(job=NULL, dbchunk=dbchunk))
    }
  } else {
    dbchunk<-data.table(objectnames=objectnames, digest=hashes, size=sizes,
                        single_object=length(objectnames)==1,
                        archive_filename=archive_filename)



    if(length(objectnames)>1) {
      obj<-as.list(obj.environment[objectnames])
    } else {
      obj<-obj.environment[[objectnames]]
    }

    job<-depwalker:::save.large.object(obj = obj, file = archive_filename, compress = compress,
                                       wait_for = wait_for, flag_use_tmp_storage = flag_use_tmp_storage,
                                       parallel_cpus = parallel_cpus, flag_detach = FALSE)
    return(list(job=job, dbchunk=dbchunk))
  }

}

# Na wejsćiu otrzymujemy listę archives list, która dla każdego archiwum zawiera listę z elementami
# objectnames - lista objektów, archive_filename - ścieżka do pliku archiwum, compress, flag_use_tmp_storage
add_runtime_objects<-function(obj.environment, archives_list, taskpath, locktimeout=NULL,
                              wait_for='save',
                              flag_use_tmp_storage=FALSE, parallel_cpus=NULL
                              )
{
  archives_db<-lists_to_df(archives_list, list_columns='objectnames')
  archives_db_flat<-data.table(dplyr::select(tidyr::unnest(archives_db), -compress, -flag_use_tmp_storage),
                               digest=NA_character_, size=NA_real_)
  archives_db_flat<-purrrlyr::by_row(archives_db_flat, ~length(.$objectnames[[1]])>1, .collate = 'cols', .to='single_object')


  objectnames<-archives_db_flat$objectnames

  for(i in seq_along(objectnames)) {
    objname<-objectnames[[i]]
    set(archives_db_flat, i, 'digest', calculate.object.digest(objname, obj.environment))
    set(archives_db_flat, i, 'size', object.size(obj.environment[[objname]]))
  }

  flag_do_sequentially=FALSE
  if(!is.null(parallel_cpus)) {
    if(parallel_cpus==0) {
      flag_do_sequentially=TRUE
    }
  }


  if(lock.exists(path, locktimeout)) {
    cat("Waiting to get the lock for ", path, "...\n")
  }
  create.lock.file(path, locktimeout)
  tryCatch({

    oldidx<-list_runtime_objects(taskpath = taskpath)

    to_remove<-dplyr::inner_join(oldidx, archives_db_flat, by=c(objectnames='objectnames'))
    #We remove objects that are going to replaced

    if(nrow(to_remove)>0) {
      browser()
      #TODO: Trzeba załatwić sytuację, gdy obiekt przechodzi z jednego kontenera do drugiego.
      #To, co robimy zależy od tego, ile jest obiektów w kontenerze, z którego obiekt jest usuwany.
      #Jeśli kontener, z którego obiekt jest usuwany, figuruje wśród kontenerów z listy archives_db,
      #To należy doczytać wszystkie pozostałe obiekty z tego kontenera, dodać go do environment,
    }



    if(flag_do_sequentially) {
      ans<-mapply(save_runtime_archive,
                   objectnames=archives_db$objectnames,
                   archive_filename=archives_db$archive_filename,
                   compress=archives_db$compress,
                   flag_use_tmp_storage=archives_db$flag_use_tmp_storage,
                   MoreArgs=list(obj.environment=obj.environment,
                                 wait_for=wait_for,
                                 parallel_cpus=parallel_cpus))
    } else {
      ans<-parallel::mcmapply(save_runtime_archive,
                               objectnames=archives_db$objectnames,
                               archive_filename=archives_db$archive_filename,
                               compress=archives_db$compress,
                               flag_use_tmp_storage=archives_db$flag_use_tmp_storage,
                               MoreArgs=list(obj.environment=obj.environment,
                                             wait_for=wait_for,
                                             parallel_cpus=parallel_cpus))
    }
    jobs<-list()
    for(i in seq_along(ans)) {
      jobs[[i]]<-ans$job
      oldidx<-rbind(filter(oldidx, archive_filename!=archive_filename), newchunk)
    }
    if(length(jobs)>0) {
      cat("Waiting for saves to finish..")
      parallel::mccollect(jobs, wait=TRUE, intermediate = function() {cat('.')})
      cat("\n")
    }
    update_runtime_objects_index(taskpath = taskpath, newidx=oldidx)
  }, finally=release.lock.file(path))
}

remove_runtime_object<-function(objname, taskpath) {
  idx<-list_runtime_objects(path)
  objdigest<-calculate.object.digest(objname, obj.environment)
  idx[[objname]]<-list(name=objname,
                       size=object.size(obj.environment[[objname]]),
                       digest=objdigest)
  update_runtime_objects_index(taskpath, idx)
}

get_runtime_index_path<-function(taskpath) {
  ext<-getOption('runtime_objects_index.extension')
  path<-pathcat::path.cat(getwd(), paste0(taskpath, ext))
  return(path)
}



#' Add additional objects to be available when the task is run. Objects will be serialized to disk when
#' metadata is saved.
#' By default all small objects will kept together, and large objects will live in a separate files.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param objects Either list or environment with the objects.
#' @param forced_save_filenames List or named character vector that forces save of the specific object (given in key) into the filepath given in value.
#'        It overrides the built in algorithm of naming and grouping the objects together in the serialized archives.
#' @param save_location Path to the alternate save location. By default the save location is the same as
#'        save location of the metadata objects, when only one file will be created, and subdirectory
#'        \code{<task_name>_runtime_objects} when more than one file will be automatically created.
#' @param forced_separate_files Character vector with the list of objects that will be forcefully serialized
#'        into a separate, dedicated archive
#' @param compress Compress method to use. Defaults to gzip.
#' @param .ignored_objects Expert argument. Character vector with the list of objects present
#'        in the \code{objects} argument, but which presence will not be tracked and saved.
#'        The code must be written in such a way, that it doesn't rely on the existance and validity
#'        of this object, e.g. this object can be used to speed things up.
#' @return modified \code{metadata} argument that includes additional runtime objects definition. Nothing will get
#'        actually saved to disk (for this use make.sure.metadata.is.saved)
#' @export
#' @seealso \code{\link{create_metadata}}, \code{\link{add_parent}}
#' @export
set_runtime_objects<-function(metadata,
                              objectnames, environment, default_save_directory_suffix='',
                              flag_forced_save_filenames=NULL,
                              forced_archive_paths=FALSE, compress='gzip')
{
  if('list' %in% class(objects)) {
    objects<-as.environment(objects)
  }
  if(!'inputobjects' %in% names(metadata)) {
    metadata$inputobjects<-list()
  }

  save_locations<-infer_save_locations(metadata = metadata, objectnames=objectnames,
                                       environment=environment,
                                       default_save_directory_suffix=default_save_directory_suffix,
                                       flag_forced_save_filenames=flag_forced_save_filenames,
                                       forced_archive_paths=forced_archive_paths,
                                       compress=compress)

  for(i in seq_along(save_locations)) {
    el<-save_locations[[i]]

    set_runtime_archive(obj.environment = environment, objectnames = i$objectnames,
                        archive_filename=i$archive_filename )
  }







  set_runtime_archive(obj.environment = environment, objectnames = objectnames)
  return(metadata)
}






#returns list with one entry for each archive. Inside the entry will be list of objects to put.
#default_save_directory_suffix - default location for each saved archive.
#flag_forced_save_filenames - either named vector with keys object_names, or vector with the size of object_names
#forced_archive_paths - either named vector with keys object_names, or vector with the size of object_names
infer_save_locations<-function(metadata, objectnames, environment, default_save_directory_suffix='',
                               flag_forced_save_filenames=NULL,
                               forced_archive_paths=NULL, compress='gzip', flag_use_tmp_storage=FALSE)
{
  browser()
  if(length(objectnames)==0) {
    return(NULL)
  }

  if(is.null(flag_forced_save_filenames)) {
    flag_forced_save_filenames<-setNames(rep(FALSE, length(objectnames)),objectnames)
  } else {
    if(is.null(names(flag_forced_save_filenames))) {
      if(length(flag_forced_save_filenames)!=length(objectnames)) {
        stop(paste0('flag_forced_save_filenames should be either named vector with keys object_names, or vector with the size of object_names'))
      }
      flag_forced_save_filenames<-setNames(flag_forced_save_filenames, objectnames)
    } else {
      values<-setNames(rep(FALSE, length(objectnames)), objectnames)
      values[names(flag_forced_save_filenames)]<-flag_forced_save_filenames
      flag_forced_save_filenames<-values
    }
  }

  if(is.null(forced_archive_paths)) {
    forced_archive_paths<-setNames(rep(NA, length(objectnames)),objectnames)
  } else {
    if(is.null(names(forced_archive_paths))) {
      if(length(forced_archive_paths)!=length(objectnames)) {
        stop(paste0('forced_archive_paths should be either named vector with keys object_names, or vector with the size of object_names'))
      }
      forced_archive_paths<-setNames(forced_archive_paths, objectnames)
    } else {
      values<-setNames(rep(FALSE, length(objectnames)), objectnames)
      values[names(forced_archive_paths)]<-forced_archive_paths
      forced_archive_paths<-values
    }
  }

  if(is.null(names(compress))) {
    if(length(compress)==1) {
      compress<-rep(compress, length(objectnames))
    } else {
      if(length(compress)!=length(compress)) {
        stop(paste0('compress should be either named vector with keys object_names, or vector with the size of object_names, or 1'))
      }
    }
    compress<-setNames(compress, objectnames)
  } else {
    values<-setNames(rep('gzip', length(objectnames)), objectnames)
    values[names(compress)]<-compress
    compress<-values
  }

  if(is.null(names(flag_use_tmp_storage))) {
    if(length(flag_use_tmp_storage)==1) {
      flag_use_tmp_storage<-rep(flag_use_tmp_storage, length(objectnames))
    } else {
      if(length(flag_use_tmp_storage)!=length(flag_use_tmp_storage)) {
        stop(paste0('flag_use_tmp_storage should be either named vector with keys object_names, or vector with the size of object_names, or 1'))
      }
    }
    flag_use_tmp_storage<-setNames(flag_use_tmp_storage, objectnames)
  } else {
    values<-setNames(rep(FALSE, length(objectnames)), objectnames)
    values[names(flag_use_tmp_storage)]<-flag_use_tmp_storage
    flag_use_tmp_storage<-values
  }


  flag_forced_save_filenames[names(forced_archive_paths)[!is.na(forced_archive_paths)] ]<-TRUE
  default_objects<-objectnames
  all_containers<-as.data.frame(table(flag_forced_save_filenames))
  out<-list()
  if(nrow(all_containers)>0) {
    for(i in seq(1, nrow(all_containers))){
      cntname<-pathcat::path.cat(dirname(metadata$path), all_containers$Var1[[i]])
      cntname<-pathcat::make.path.relative(dirname(metadata$path), cntname)
      poss<-which(flag_forced_save_filenames==cntname)
      cnt_objnames<-objectnames[poss]
      out[[cntname]]<-list(objectnames=cnt_objnames, archive_filename=cntname,
                           compress=compress[poss],
                           flag_use_tmp_storage=flag_use_tmp_storage[poss])
      default_objects<-setdiff(default_objects, cnt_objnames)
    }
  }

  if(length(default_objects)>0) {
    flag_forced_save_filenames<-flag_forced_save_filenames[default_objects]
    compress<-compress[default_objects]
    flag_use_tmp_storage<-flag_use_tmp_storage[default_objects]
    objectnames<-default_objects
    objectsizes<-purrr::map_dbl(objectnames, ~object.size(environment[[.]]))
    flag_forced_save_filenames[objectsizes > getOption('tune.threshold_objsize_for_dedicated_archive')]<-TRUE
    number_of_files<-sum(flag_forced_save_filenames)
    if(sum(!flag_forced_save_filenames)>0) {
      if(sum(flag_forced_save_filenames)==0) {
        generic_file_name<-paste0(pathcat::path.cat(dirname(metadata$path), default_save_directory_suffix), basename(metadata$path), ".io.rds")
      } else {
        if(default_save_directory_suffix!='') {
          generic_file_name<-paste0(pathcat::path.cat(dirname(metadata$path), default_save_directory_suffix), basename(metadata$path), ".io.rds")
        } else {
          generic_file_name<-paste0(dirname(metadata$path), "_runtime_objects/default_container.rds")
        }
        generic_file_name<-pathcat::make.path.relative(dirname(metadata$path), generic_file_name)

        out[[generic_file_name]]<-c(
          out[[generic_file_name]],
          list(objectnames=objectnames[!flag_forced_save_filenames],
               archive_filename=generic_file_name,
               compress=compress[!flag_forced_save_filenames],
               flag_use_tmp_storage=flag_use_tmp_storage[!flag_forced_save_filenames]))
      }
    }

    if(sum(flag_forced_save_filenames)>0) {
      separate_objects<-objectnames[flag_forced_save_filenames]
      separate_paths<-paste0(pathcat::path.cat(dirname(metadata$path),default_save_directory_suffix, "_runtime_objects"),
                             paste0(separate_objects, ".rds"))
      separate_compress<-compress[flag_forced_save_filenames]
      separate_flag_use_tmp_storage<-flag_use_tmp_storage[flag_forced_save_filenames]

      for(i in seq(1, length(separate_objects))) {
        obj_name<-separate_objects[[i]]
        path<-separate_paths[[i]]
        out[[path]]<-c(
          out[[path]],
          list(objectnames=obj_name, archive_filename=path,
               compress=separate_compress[[i]],
               flag_use_tmp_storage=separate_flag_use_tmp_storage[[i]]))
      }
    }
  }

  df<-as.data.frame(table(names(out)))
  dups<-dplyr::filter(df, Freq>1)
  if(nrow(dups)>0){
    nondups<-dplyr::filter(df, Freq==1)
    if(nrow(nondups)>0) {
      newout<-out[nondups$Var1]
    } else {
      newout<-list()
    }
    for(i in seq(1, nrow(dups))) {
      cname<-dups$Var1[[i]]
      poss<-which(names(out)==cname)
      dic<-out[poss]

      dicdf<-list_to_df(dic, list_columns=setdiff(names(dic), 'archive_filename'))
      outnew[[cname]]<-list(objectnames=do.call(c, dicdf$objectnames),
                            archive_filename=cname,
                            compress=do.call(c,dicdf$compress),
                            flag_use_tmp_storage=do.call(c,dicdf$flag_use_tmp_storage)
                            )
    }
    out<-newout
  }

  for(i in length(out)) {
    item<-out[[i]]
    if(length(unique(item$compress))!=length(item$compress)) {
      stop(paste0("Non-unique elements in the argument compress for runtime.object saved in ",
                  item$archive_filename))
    }
    if(length(unique(item$flag_use_tmp_storage))!=length(item$flag_use_tmp_storage)) {
      stop(paste0("Non-unique elements in the argument flag_use_tmp_storage for runtime.object saved in ",
                  item$archive_filename))
    }
    out[[i]]<-list(archive_filename=item$archive_filename,
                   objectnames=item$objectnames,
                   compress=item$compress[[1]],
                   flag_use_tmp_storage=item$flag_use_tmp_storage[[1]])
  }

  return(out)
}



#' Add additional objects to be available when the task is run. Objects will be serialized to disk when
#' metadata is saved.
#' By default all small objects will kept together, and large objects will live in a separate files.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param objects Either list or environment with the objects.
#' @param forced_save_filenames List or named character vector that forces save of the specific object (given in key) into the filepath given in value.
#'        It overrides the built in algorithm of naming and grouping the objects together in the serialized archives.
#' @param save_location Path to the alternate save location. By default the save location is the same as
#'        save location of the metadata objects, when only one file will be created, and subdirectory
#'        \code{<task_name>_runtime_objects} when more than one file will be automatically created.
#' @param forced_separate_files Character vector with the list of objects that will be forcefully serialized
#'        into a separate, dedicated archive
#' @param compress Compress method to use. Defaults to gzip.
#' @param .ignored_objects Expert argument. Character vector with the list of objects present
#'        in the \code{objects} argument, but which presence will not be tracked and saved.
#'        The code must be written in such a way, that it doesn't rely on the existance and validity
#'        of this object, e.g. this object can be used to speed things up.
#' @return modified \code{metadata} argument that includes additional runtime objects definition. Nothing will get
#'        actually saved to disk (for this use make.sure.metadata.is.saved)
#' @export
#' @seealso \code{\link{create_metadata}}, \code{\link{add_parent}}
#' @export
add_runtime_objects<-function(metadata, objects, save_location=NULL, forced_save_filenames=character(0),
                              forced_separate_files, compress='gzip', .ignored_objects=character(0))
{
  if('list' %in% class(objects)) {
    objects<-as.environment(objects)
  }
  if(!'inputobjects' %in% names(metadata)) {
    metadata$inputobjects<-list()
  }


  #First we add ignored objects, as there is the least amount of work to do with them
  objectnames<-intersect(names(objects), .ignored_objects)
  if(length(objectnames)>0) {
    for(on in objectnames) {
      metadata$inputobjects[[on]]<-list(name=on, ignored=TRUE)
    }
  }

  #Now we focus on the non-ignored objects
  objectnames<-setdiff(names(objects), .ignored_objects)

  objectsizes<-purrr::map_dbl(objectnames, ~object.size(objects[[.]]))

  #Objects bigger than 5kb will be put in the separate containers.
  #separate_containers is a flag for each object specifying whether to use a separate container for it.
  separate_containers<-objectnames %in% c(names(forced_save_filenames), forced_separate_files) |
    objectsizes > getOption('tune.threshold_objsize_for_dedicated_archive')

  number_of_files<-sum(separate_containers)

  if(number_of_files==0) {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects.rds")
  } else {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects/default_container.rds")
    separate_objects<-objectnames[separate_containers]
    separate_paths<-paste0(basename(metadata$path), "_runtime_objects/_", separate_objects, ".rds")
    if(length(forced_separate_files)>0) {
      for(i in seq(1, length(forced_separate_files))) {
        obj_name<-names(forced_separate_files)[[i]]
        if(obj_name %in% names(forced_separate_files)){
          path<-forced_separate_files[[obj_name]]
          pos<-which(obj_name == separate_objects)
          separate_paths[[pos]]<-path
        }
      }
    }
  }
  jobs<-list()

  if(any(!separate_containers)) {
    #Create entries for each small object in the shared container
    relative_path<-path=pathcat::make.path.relative(metadata$path, generic_file_name)
    objnames<-objectnames[which(!separate_containers)]
    objsizes<-objectsizes[which(!separate_containers)]
    objdigests<-purrr::map_chr(objnames, ~digest::digest(objects[[.]]))

    metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                                 path=relative_path,
                                                 compress=compress, objectdigest=objectdigest,
                                                 size=objsizes)
  }
  if(any(separate_containers)) {
    poss<-which(separate_containers)
    for(pos in poss) {
      objectname<-objectnames[[pos]]
      objsize<-objectsizes[[pos]]
      relative_path<-path=pathcat::make.path.relative(metadata$path, separate_paths)
      objdigest<-digest::digest(objects[[objectname]])


      metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                                   path=relative_path,
                                                   compress=compress, objectdigest=objectdigest,
                                                   size=objsize)
    }
  }

  return(metadata)
}



#' Add additional objects to be available when the task is run. Objects will be serialized to disk when
#' metadata is saved.
#' By default all small objects will kept together, and large objects will live in a separate files.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param objects Either list or environment with the objects.
#' @param forced_save_filenames List or named character vector that forces save of the specific object (given in key) into the filepath given in value.
#'        It overrides the built in algorithm of naming and grouping the objects together in the serialized archives.
#' @param save_location Path to the alternate save location. By default the save location is the same as
#'        save location of the metadata objects, when only one file will be created, and subdirectory
#'        \code{<task_name>_runtime_objects} when more than one file will be automatically created.
#' @param forced_separate_files Character vector with the list of objects that will be forcefully serialized
#'        into a separate, dedicated archive
#' @param compress Compress method to use. Defaults to gzip.
#' @param .ignored_objects Expert argument. Character vector with the list of objects present
#'        in the \code{objects} argument, but which presence will not be tracked and saved.
#'        The code must be written in such a way, that it doesn't rely on the existance and validity
#'        of this object, e.g. this object can be used to speed things up.
#' @return modified \code{metadata} argument that includes additional runtime objects definition. Nothing will get
#'        actually saved to disk (for this use make.sure.metadata.is.saved)
#' @export
#' @seealso \code{\link{create_metadata}}, \code{\link{add_parent}}
#' @export
add_ignored_runtime_objects<-function(metadata, objects)
{
  if('list' %in% class(objects)) {
    objects<-as.environment(objects)
  }
  if(!'inputobjects' %in% names(metadata)) {
    metadata$inputobjects<-list()
  }


  #First we add ignored objects, as there is the least amount of work to do with them
  objectnames<-intersect(names(objects), .ignored_objects)
  if(length(objectnames)>0) {
    for(on in objectnames) {
      metadata$inputobjects[[on]]<-list(name=on, ignored=TRUE)
    }
  }

  #Now we focus on the non-ignored objects
  objectnames<-setdiff(names(objects), .ignored_objects)

  objectsizes<-purrr::map_dbl(objectnames, ~object.size(objects[[.]]))

  #Objects bigger than 5kb will be put in the separate containers.
  #separate_containers is a flag for each object specifying whether to use a separate container for it.
  separate_containers<-objectnames %in% c(names(forced_save_filenames), forced_separate_files) |
    objectsizes > getOption('tune.threshold_objsize_for_dedicated_archive')

  number_of_files<-sum(separate_containers)

  if(number_of_files==0) {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects.rds")
  } else {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects/default_container.rds")
    separate_objects<-objectnames[separate_containers]
    separate_paths<-paste0(basename(metadata$path), "_runtime_objects/_", separate_objects, ".rds")
    if(length(forced_separate_files)>0) {
      for(i in seq(1, length(forced_separate_files))) {
        obj_name<-names(forced_separate_files)[[i]]
        if(obj_name %in% names(forced_separate_files)){
          path<-forced_separate_files[[obj_name]]
          pos<-which(obj_name == separate_objects)
          separate_paths[[pos]]<-path
        }
      }
    }
  }
  jobs<-list()

  if(any(!separate_containers)) {
    #Create entries for each small object in the shared container
    relative_path<-path=pathcat::make.path.relative(metadata$path, generic_file_name)
    objnames<-objectnames[which(!separate_containers)]
    objsizes<-objectsizes[which(!separate_containers)]
    objdigests<-purrr::map_chr(objnames, ~digest::digest(objects[[.]]))

    metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                                 path=relative_path,
                                                 compress=compress, objectdigest=objectdigest,
                                                 size=objsizes)
  }
  if(any(separate_containers)) {
    poss<-which(separate_containers)
    for(pos in poss) {
      objectname<-objectnames[[pos]]
      objsize<-objectsizes[[pos]]
      relative_path<-path=pathcat::make.path.relative(metadata$path, separate_paths)
      objdigest<-digest::digest(objects[[objectname]])


      metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                                   path=relative_path,
                                                   compress=compress, objectdigest=objectdigest,
                                                   size=objsize)
    }
  }

  return(metadata)
}






#' Add additional objects to be available when the task is run. Objects will be serialized to disk when
#' metadata is saved.
#' By default all small objects will kept together, and large objects will live in a separate files.
#'
#' @param metadata already created metadata you wish to add the source file to. You can create task's metadata from scratch with \code{\link{create.metadata}}.
#' @param objects Either list or environment with the objects.
#' @param forced_save_filenames List or named character vector that forces save of the specific object (given in key) into the filepath given in value.
#'        It overrides the built in algorithm of naming and grouping the objects together in the serialized archives.
#' @param save_location Path to the alternate save location. By default the save location is the same as
#'        save location of the metadata objects, when only one file will be created, and subdirectory
#'        \code{<task_name>_runtime_objects} when more than one file will be automatically created.
#' @param forced_separate_files Character vector with the list of objects that will be forcefully serialized
#'        into a separate, dedicated archive
#' @param compress Compress method to use. Defaults to gzip.
#' @param .ignored_objects Expert argument. Character vector with the list of objects present
#'        in the \code{objects} argument, but which presence will not be tracked and saved.
#'        The code must be written in such a way, that it doesn't rely on the existance and validity
#'        of this object, e.g. this object can be used to speed things up.
#' @return modified \code{metadata} argument that includes additional runtime objects definition. Nothing will get
#'        actually saved to disk (for this use make.sure.metadata.is.saved)
#' @export
#' @seealso \code{\link{create_metadata}}, \code{\link{add_parent}}
#' @export
remove_runtime_objects<-function(metadata, objects)
{
  if('list' %in% class(objects)) {
    objects<-as.environment(objects)
  }
  if(!'inputobjects' %in% names(metadata)) {
    metadata$inputobjects<-list()
  }


  #First we add ignored objects, as there is the least amount of work to do with them
  objectnames<-intersect(names(objects), .ignored_objects)
  if(length(objectnames)>0) {
    for(on in objectnames) {
      metadata$inputobjects[[on]]<-list(name=on, ignored=TRUE)
    }
  }

  #Now we focus on the non-ignored objects
  objectnames<-setdiff(names(objects), .ignored_objects)

  objectsizes<-purrr::map_dbl(objectnames, ~object.size(objects[[.]]))

  #Objects bigger than 5kb will be put in the separate containers.
  #separate_containers is a flag for each object specifying whether to use a separate container for it.
  separate_containers<-objectnames %in% c(names(forced_save_filenames), forced_separate_files) |
    objectsizes > getOption('tune.threshold_objsize_for_dedicated_archive')

  number_of_files<-sum(separate_containers)

  if(number_of_files==0) {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects.rds")
  } else {
    generic_file_name<-paste0(basename(metadata$path), "_runtime_objects/default_container.rds")
    separate_objects<-objectnames[separate_containers]
    separate_paths<-paste0(basename(metadata$path), "_runtime_objects/_", separate_objects, ".rds")
    if(length(forced_separate_files)>0) {
      for(i in seq(1, length(forced_separate_files))) {
        obj_name<-names(forced_separate_files)[[i]]
        if(obj_name %in% names(forced_separate_files)){
          path<-forced_separate_files[[obj_name]]
          pos<-which(obj_name == separate_objects)
          separate_paths[[pos]]<-path
        }
      }
    }
  }
  jobs<-list()

  if(any(!separate_containers)) {
    #Create entries for each small object in the shared container
    relative_path<-path=pathcat::make.path.relative(metadata$path, generic_file_name)
    objnames<-objectnames[which(!separate_containers)]
    objsizes<-objectsizes[which(!separate_containers)]
    objdigests<-purrr::map_chr(objnames, ~digest::digest(objects[[.]]))

    metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                                 path=relative_path,
                                                 compress=compress, objectdigest=objectdigest,
                                                 size=objsizes)
  }
  if(any(separate_containers)) {
    poss<-which(separate_containers)
    for(pos in poss) {
      objectname<-objectnames[[pos]]
      objsize<-objectsizes[[pos]]
      relative_path<-path=pathcat::make.path.relative(metadata$path, separate_paths)
      objdigest<-digest::digest(objects[[objectname]])


      metadata$inputobjects[[relative_path]]<-list(name=objnames, ignored=FALSE,
                                                   path=relative_path,
                                                   compress=compress, objectdigest=objectdigest,
                                                   size=objsize)
    }
  }

  return(metadata)
}


