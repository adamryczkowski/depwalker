capture.evaluate<-function(code, envir=.GlobalEnv)
{
  is_error<-FALSE

  err_handler<-function(msg)
  {
    sc<-sys.calls()
    pos=max(which(!is.na(match(lapply(sc,deparse),'eval(expr, envir, enclos)'))))
    b<-sc[(pos+1):(length(sc)-3)]
    cat(paste0("\nError: ", msg$message, " on ", deparse(msg$call)))
    cat("\nTrace:\n")
    mytraceback(b)
    is_error<<-TRUE
  }
  #debugonce(err_handler)

  msg_handler<-function(msg)
  {
    sc<-sys.calls()
    pos=max(which(!is.na(match(lapply(sc,deparse),'eval(expr, envir, enclos)'))))
    b<-sc[(pos+1):(length(sc)-6)]
    cat(paste0("\nMessage: ", msg$message, " on ", deparse(msg$call)))
    cat("\nTrace:\n")
    mytraceback(b)
  }

  wrn_handler<-function(msg)
  {
    sc<-sys.calls()
    pos=max(which(!is.na(match(lapply(sc,deparse),'eval(expr, envir, enclos)'))))
    b<-sc[(pos+1):(length(sc)-6)]
    cat(paste0("\nWarning: ", msg$message, " on ", deparse(msg$call)))
    cat("\nTrace:\n")
    mytraceback(b)
  }
  oh<-evaluate::new_output_handler(error=err_handler, message=msg_handler, warning=wrn_handler)

  out<-evaluate::evaluate(code, output_handler=oh, envir=envir, stop_on_error=1L, include_timing=TRUE, debug=FALSE)
  out<-utils::capture.output(evaluate::replay(out))
  return(list(output=out, is.error=is_error))
}

run.script<-function(metadata, objects.to.keep,estimation.only=NULL)
{
  if (!is.logical(estimation.only))
  {
    estimation.only$script.time<-script.time(metadata)
    return(estimation.only)
  }


  outfile=pathcat::path.cat(getwd(), paste0(metadata$path,getOption('echo.extension')))
  if (file.exists(outfile))
    unlink(outfile)
  errfile=pathcat::path.cat(getwd(), paste0(metadata$path,getOption('error.extension')))
  if (file.exists(errfile))
    unlink(errfile)

  vars.before<-c(ls(envir=.GlobalEnv, all.names = TRUE), objects.to.keep)
  gc()
  busycpus<-cpu.usage.list()$busy.cpus
  fmem<-memfree()
  olddir<-getwd()
  if (!is.null(metadata$execution.directory))
  {
    setwd(get.codepath(metadata, metadata$execution.directory))
  }
  time<-as.numeric(system.time(
    out<-capture.evaluate(metadata$code, envir=.GlobalEnv)
  ))[1:3]
  coresinfo<-cpu.count()
  gc()
  if (out$is.error)
  {
    con<-file(errfile)
  } else
  {
    con<-file(outfile)
  }
  writeLines(out$output, con)
  close(con)
  if (!is.null(metadata$execution.directory))
  {
    setwd(olddir)
  }
  timecost<-list(walltime=bit64::as.integer64(time[3]*1000),
                 cputime=bit64::as.integer64(time[1]*1000),
                 systemtime=bit64::as.integer64(time[2]*1000),
                 cpumodel=sfsmisc::Sys.cpuinfo()[[5]],
                 membefore=as.integer(fmem),
                 memafter=as.integer(memfree()),
                 corecount=as.integer(coresinfo$core.count),
                 virtualcorecount=as.integer(coresinfo$virtual.core.count), busycpus=as.integer(busycpus))
  if (is.null(metadata$timecosts))
    metadata$timecosts<-data.table::as.data.table(timecost)
  else
    metadata$timecosts<-rbind(metadata$timecosts, timecost)
  vars.after<-ls(envir=.GlobalEnv, all.names = TRUE)
  vars.to.delete<-setdiff(setdiff(vars.after, vars.before), objects.to.keep)
  if (length(vars.to.delete)>0)
    rm(list=vars.to.delete, envir=.GlobalEnv)

  existances<-sapply(objects.to.keep, function(n)exists(n, envir=.GlobalEnv))
  if (!all(existances))
    return(NULL)

  return(metadata)
}
