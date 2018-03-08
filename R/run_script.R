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

run_script<-function(metadata, objects_to_keep, estimation_only=NULL, run_environment=NULL, flag_do_gc=TRUE)
{
  if (!is.logical(estimation_only))
  {
    estimation_only$script.time<-script.time(metadata)
    return(estimation_only)
  }

  # outfile=pathcat::path.cat(getwd(), paste0(metadata$path,getOption('depwalker.echo_extension')))
  # if (file.exists(outfile))
  #   unlink(outfile)
  # errfile=pathcat::path.cat(getwd(), paste0(metadata$path,getOption('depwalker.error_extension')))
  # if (file.exists(errfile))
  #   unlink(errfile)

  vars.before<-c(ls(envir=run_environment, all.names = TRUE), objects_to_keep)
  if(flag_do_gc) {
    gc()
  }
  #busycpus<-cpu.usage.list()$busy.cpus #It takes too long to compute
  fmem<-memfree()
  olddir<-getwd()
  if (!is.null(metadata$execution_directory))
  {
    setwd(get_path(metadata, metadata$execution_directory))
  }

#  browser()
  code<-get_main_code(metadata)
  timestamp<-Sys.time()

  time<-as.numeric(system.time(
    out<-capture.evaluate(code, envir=run_environment)
  ))[1:3]
  coresinfo<-cpu_count()
  if(flag_do_gc) {
    gc()
  }
  if (!is.null(metadata$execution_directory))
  {
    setwd(olddir)
  }
  metadata<-append_history_record(metadata,
                                  timestamp=timestamp,
                                  walltime=time[3]*1000,
                                  cputime=time[1]*1000,
                                  systemtime=time[2]*1000,
                                  cpumodel=sfsmisc::Sys.cpuinfo()[[5]],
                                  membefore=as.integer(fmem),
                                  memafter=as.integer(memfree()),
                                  corecount=as.integer(coresinfo$core.count),
                                  virtualcorecount=as.integer(coresinfo$virtual.core.count),
                                  output=out$output,
                                  flag_success=!out$is.error)

  vars.after<-ls(envir=run_environment, all.names = TRUE)
  vars.to.delete<-setdiff(setdiff(vars.after, vars.before), objects_to_keep)
  if (length(vars.to.delete)>0)
    rm(list=vars.to.delete, envir=run_environment)

  existances<-sapply(objects_to_keep, function(n)exists(n, envir=run_environment))
  if (!all(existances))
    return(NULL)

  return(metadata)
}
