print_m<-function(m) {
  basename('/home/Adama-docs/Adam/MyDocs/Statystyka/Maszyny/Dab2/depwalker')
  txt<-paste0("Task ", basename(m$path))
  if(length(m$objectrecords)==0) {
    txt<-paste0(txt, " generating no objects")
  } else {
    obj_txt<-format_objectrecs(m)
    txt<-paste0(txt, " generating ", obj_txt)
  }
  txt<-paste0(txt, '\n')
  obj_code<-formatted_source(m$code)
  if(length(obj_code)>0) {
    txt<-paste0(txt, '```\n', obj_code, '\n```\n')
  }


}

formatted_source<-function(code, flag.include.comments=FALSE, width.cutoff=getOption("width"), max_length=30) {
  txt<-formatR::tidy_source(text=code, output = FALSE, comment = flag.include.comments, blank = FALSE, arrow = TRUE, width.cutoff = width.cutoff)
  txt<-unlist(stringr::str_split(txt$text.tidy, pattern = stringr::fixed("\n")))
  if(length(txt)>30) {
    return(c(txt[1:10],'...'))
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
  parents<-purrr::map_chr(m$parents, ~.$task)
}

format_parent<-function(m, p) {

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

    ors<-dplyr::select(dplyr::arrange(ors, -size_num), -size_num)
    setattr(ors$txt, 'decoration','')
    setattr(ors$size, 'decoration','')
    ans<-danesurowe::format_item_list_en(ors, txt_attribute_bare_quote ='')

  } else {
    ans<-''
  }
  return(ans)
}
