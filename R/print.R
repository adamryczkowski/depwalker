print_m<-function(m) {
  txt<-paste0("Task ", basename(m$path))
  if(length(m$objectrecords)==0) {
    txt<-paste0(txt, " generating no objects")
  } else {
    obj_txt<-depwalker:::format_objectrecs(m)
    txt<-paste0(txt, " generating ", obj_txt)
  }
  txt<-paste0(txt, '\n')

  if(length(m$parents)>0) {
    txt<-paste0(txt, "\nDependencies:\n", depwalker:::format_parents(m), '\n')
  }

  obj_code<-depwalker:::formatted_source(m$code)
  if(length(obj_code)>0) {
    txt<-paste0(txt, '\n```\n', paste0(obj_code, collapse = '\n'), '\n```\n')
  }

  if(length(m$extrasources)>0) {
    for(es in m$extrasources) {
      path<-depwalker:::get.objectpath(objectrecord = es, metadata = m)
      if(file.exists(path)) {
        src<-readLines(path)
        if(es$flag.r) {
          src<-depwalker:::formatted_source(src)
        } else {
          if(length(src)>30) {
            src<-c(src[1:10],'...', src[seq(length(src)-4, length(src))])
          }
        }
        obj_code<-paste0('```\n', paste0(src, collapse='\n'), '\n```\n')
      } else {
        obj_code<-paste0("File ", path, " doesn't exist!\n")
      }
      txt<-paste0(txt, es$path, ':\n', obj_code, '\n\n')
    }
  }

  if(m$execution.directory!='') {
    txt<-paste0(txt, 'execution directory: ', m$execution.directory)
  }

  return(txt)
}

formatted_source<-function(code, flag.include.comments=FALSE, width.cutoff=getOption("width"), max_length=30) {
  txt<-formatR::tidy_source(text=code, output = FALSE, comment = flag.include.comments, blank = FALSE, arrow = TRUE, width.cutoff = width.cutoff)
  txt<-unlist(stringr::str_split(txt$text.tidy, pattern = stringr::fixed("\n")))
  if(length(txt)>30) {
    return(c(txt[1:10],'...', txt[seq(length(txt)-4, length(txt))]))
  } else {
    return(txt)
  }
}

format_objectrec<-function(m, or) {
  p<-paste0(depwalker:::get.objectpath(or, m), getOption('object.save.extension'))
  txt<-basename(or$name)
  size=NA
  size_txt=NA

  if(file.exists(p)) {
    fs<-file.size(p)
    size=fs
    fs<-utils:::format.object_size(fs, "auto")
    size_txt<-fs
  }
  return(list(txt=txt, size=size, size_txt=size_txt))
}

format_parents<-function(m) {
  ps<-data.table(name=rep(NA_character_, length(m$parents)),
                 objects=rep(NA_character_, length(m$parents)))
  for(i in seq(1, length(m$parents))) {
    p<-m$parents[[i]]
    data.table::set(ps, i, 'name', m$parents[[i]]$path)
    objects<-depwalker:::format_parent(m, p)
    data.table::set(ps, i, 'objects', objects)
  }
  setattr(ps$name, 'decoration','')
  setattr(ps$objects, 'decoration','')
  ans<-danesurowe::format_item_list_en(ps, txt_attribute_bare_quote ='')

  return(ans)
}

format_parent<-function(m, p) {
#  txt<-paste0(p$path, ': ')
  txt<-''
  if(length(p$name)>0) {
    os<-rep(NA_character_, length(p$name))
    for(i in seq(1, length(p$name))) {
      if(p$aliasname[[i]]!=p$name[[i]]) {
        os[[i]]<-paste0(p$name[[i]], "->", p$aliasname[[i]])
      } else {
        os[[i]]<-p$name[[i]]
      }
    }
    ans<-danesurowe::format_item_list_en(os, txt_separator_last=', ')
    txt<-paste0(txt, ans)
  }
  return(txt)
}

format_objectrecs<-function(m) {
  if(length(m$objectrecords)>0) {
    ors<-data.table(txt=rep(NA_character_, length(m$objectrecords)),
                    size_num=rep(NA_real_, length(m$objectrecords)),
                    size=rep(NA_character_, length(m$objectrecords)))

    for(i in seq(1, length(m$objectrecords))) {
      or<-format_objectrec(m, m$objectrecords[[i]])
      data.table::set(ors, i, 'txt', or$txt)
      data.table::set(ors, i, 'size_num', or$size)
      data.table::set(ors, i, 'size', or$size_txt)
    }

    if(all(is.na(ors$size_num))) {
      ors<-dplyr::select(ors, -size_num, -size)
    } else {
      ors<-dplyr::select(dplyr::arrange(ors, -size_num), -size_num)
      setattr(ors$size, 'decoration','')
    }

    setattr(ors$txt, 'decoration','')
    ans<-danesurowe::format_item_list_en(ors, txt_attribute_bare_quote ='')

  } else {
    ans<-''
  }
  return(ans)
}
