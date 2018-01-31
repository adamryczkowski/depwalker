#' depwalker: A dependency walker, that manage execution of user scripts.
#'
#'Depwalker tracks dependencies between the scripts trying to satisfy them in parallel,
#'caches the results for later fast retrieval, gives detailed execution log and performance metrics.
#'
#' @section Rationale:
#' The \code{depwalker} can help you, if you write complex statistical procedures for your data, that need to be
#' repeated each time when part of the input information changes, and if your computation take non-trivial amount of time
#' and resources.
#'
#'
#' Imagine you do a series of binomial regressions on some big dataset. You may have the following scripts doing various tasks:
#' \enumerate{
#' \item Reading the database from source.
#' \item Doing descriptive statistics on the database.
#' \item Doing WoE (Weight of Evidence) coding on all pre-selected independent variables
#' \item Doing a logistic regression and presenting its results
#' \item Refining results of the regression
#' }
#'
#' Steps 1 and 2 take only database as input. Step 3 requires completion of Step 1, and input of pre-selected
#' dependent variables (which may be put in e.g. CSV file, be result of another script or be hard-coded
#' in the R code of the script 3 itself). Step 4 requires the WoE-coded database from Step 3 and dependent variable from
#' Step 1.
#'
#' After you set up all tasks in \code{depwalker}, when you want to display regression results from step 5 all
#' you need to do, is to require results from step 5 with something like \code{load.object('/path/to/step 5')}.
#'
#' Computer will take the shortest and fastest possible path to bring you the current results. You don't need to worry
#' to remember to re-do WoE coding after you added an extra independent variable. You don't need to wait the whole
#' process, if you only changed regression parameters or didn't change anything at all.
#'
#' @section User scripts:
#'
#' User scripts are pieces of R code, that are declared in \code{depwalker} as a certain task.
#' User scripts can be run for their side effects, or for the objects they create in R memory. In the latter case,
#' you need to declare all target objects of your script as well, with \code{add.objectrecord(...)}.
#' All non-declared objects will be purged after
#' the script run to save precious memory. Every declared target object can be used as a dependency for another
#' script you may write. User scripts may need certain objects to be already present in the R memory. In such case
#' you should declare dependencies ('parents', 'ancestors') to your scripts with \code{add.parent(...)}.
#' Every time, when you require run of your script and cached version of the objects it produces
#' are not available, it will first executeall dependencies (or just load a cached version of them).
#'
#' @section Optimizations:
#'
#' Scripts are not executed, if there exists current valid cached version of the objects they produce, and that
#' reading the cache is actually faster, than execution the script (which may not be the case, if the script
#' reads the database from external source, or creates a lot of data in short time).
#'
#' If a script has more than one dependency, all of them are executed in parallel (or at least as many, as memory allows)
#'
#' After the object is created via execution of user script, it gets cached - i.e. saved on disk, to speed up
#' subsequent computations. Saving a large object can be time consuming, so by default it is done in parallel.
#'
#' @section YAML task metadata file format:
#'
#' Each task is represented by a metadata file written in YAML format that includes all necessery information plus
#' run statistics. The file consists of the following sections. Fields that define task are \strong{bold}.
#' If any of theese fields changes (or data the field
#' points to) the whole task is treated as changed and its cached objects will be treated invalid until next run is
#' executed.
#' \describe{
#' \item{\strong{path}}{Only in memory. Path with the saved version of this metadata.
#'                      It is also a folder that all relative paths are based on.
#'                      For metadata only in memory, it can be relative, otherwise it is absolute.
#'                      If relative, it is assumed to be relative to the current directory}
#' \item{\strong{inputfiles}}{Optional dictionary with all the external files managed by the main R script.
#'       These may be source files
#'       in other languages like C++, or CSV files with data. Dictionary key is case-sensitive base file name.
#'       These source files (written in any language) \emph{are expected to be invoked from within the
#'       main script file (\code{codepath})} and will not be called automatically.
#'       There are tracked for changes, and if any of the secondary source files changes,
#'       the whole task will need to be re-run. }
#' \item{\strong{inputobjects}}{Dictionary with all objects that are initialized on task creation, with contents saved
#'       on disk and loaded and placed in the run environment when the task is about to be executed.}
#' \item{inputobjects_storage}{Path relative to the path of the metadata, with the objectstorage for
#'       the inputobjects}
#' \item{\strong{parents}}{Dictionary with all parent task definitions. Dictionary key is base name of the task. }
#' \item{\strong{objectrecords}}{Dictionary with registered objects produced by the main R script.
#'       Dictionary key is the object name. }
#' \item{objectrecords_storage}{Path relative to the path of the metadata, with the objectstorage for
#'       the objectrecords}
#' \item{runtime_environment}{Only runtime. Environment with loaded version of all or some objects defined in \code{inputobjects} member.}
#' \item{\strong{execution_directory}}{String with the directory when the script should be run. Can be NULL (or absent),
#'       or string with the directory. Directory can be absolute, or relative to the task's directory.
#'       e.g. execution.directory equal '' will mean the task's main directory.}
#' \item{flag_never_execute_parallel}{Boolean flag. If set, this task will never be executed in parallel to the main
#'       R task. Mostly for debugging purposes}
#' \item{flag_force_recalculation}{Boolean flag. If set, next time the task will be executed regardless whether
#'       there is update version in the cache. After recalculation this flag will be cleared automatically.}
#' \item{flag_use_tmp_storage}{Boolean flag. If set, it indicates that the task's path is slow to write, and
#'       all large uncompressed files will be first saved into the \code{/tmp}, and then saved into the
#'       destination path.}
#' \item{history}{List with run statistics and results of this task.}
#' }
#'
#' \strong{Description of the \code{inputfiles} item}:
#' \describe{
#' \item{\strong{path}}{Path to the file. Can be absoulute or relative to the task metadata file.}
#' \item{code}{Optional and only runtime. May contain the actual contents of the file. Note, that
#'       before run all inputfiles except for RMain will get saved to disk, because that is the only way
#'       the main script could reach them.}
#' \item{flag_checksum}{Flag. If set, contents of the file will be checksummed and on any change of the contents, the task
#'       will be reexecuted. If cleared, only change of the filename will be considered when checking whether the cached
#'       task results are valid.}
#' \item{type}{Tells type of the file. Possible values: \code{R}, \code{RMain}, \code{txt}, \code{binary}.
#'             Value determines type of digest (binary/text), type of output formatting (txt/R/binary) or
#'             if it is a main executable scrupt (RMain/other)}
#' \item{\strong{digest}}{Digest of this source file calculated either in binary or text mode}
#' }
#'
#' \strong{Description of the \code{inputobject} item}:
#' \describe{
#' \item{\strong{name}}{Name of the object in the run environment.}
#' \item{ignored} A flag. If true, then object with this name will never be tracked and all the subsequent
#'                elements will be ignored. It is designed for handling optional arguments which presents
#'                only affects the performance, such as pre-loaded database, which in case it is absent
#'                can easily be loaded from disk.
#' \item{\strong{digest}} Digest of the object
#'}
#'
#' \strong{Description of the \code{parentrecord} item}:
#' \describe{
#' \item{path}{Path (either absoulute or relative) to the imported task's metadata file.}
#' \item{\strong{names}}{Names of the imported objects.}
#' \item{\strong{aliasnames}}{Alternate names of each object, set if our script needs the object produced
#'       by the imported task in different name.}
#' \item{\strong(objectdigests}}{Digests of the parent objects, filled in when the parent object is run,
#'       to freeze the parent's state and react
#'       when the parent has changed, invalidating our object}
#' }
#'
#' \strong{Description of the \code{objectrecord} item}:
#' \describe{
#' \item{\strong{name}}{Name of object produced by the main R script.}
#' \item{archivepath}{Optional path to the archive location, where the cached version of the object should be saved
#'       after execution of the script.
#'       For large objects it can point to the alternate storage or outside of the
#'       version-controlled/synchronized directory}
#' \item{compress}{Compression method. Currently the only methods supported are:
#'       \code{xz}, \code{bzip2}, \code{gzip} and \code{false}. Defaults to \code{xz}}
#' \item{\strong{digest}} Digest of the object. Important when this task is a parent.
#'}
#'
#' \strong{Description of the \code{history} item}:
#' \describe{
#' \item{timestamp}{Numeric. Serial number of date and time of the run, in GMT}
#' \item{walltime}{Execution wall time for the script}
#' \item{cputime}{Execution cpu time of the execution of the script. May be bigger or smaller from the walltime}
#' \item{systemtime}{Kernel-mode cpu time of the execution of the script. Big values may indicate an OS bottleneck.}
#' \item{cpumodel}{String indicating model of the CPU the script was running on}
#' \item{membefore}{Free mem before running the script}
#' \item{memafter}{Free mem after running the script. Difference with \code{membefore} may indicate memory leaks.}
#' \item{corecount}{Number of physical cores of the CPU}
#' \item{virtualcorecount}{Number of independent execution threads of the CPU}
#' \item{output}{Only in memory. Debug output of the run. It is a character vector. On metadata save it will
#'       be dumped to the text file.}
#' \item{flag_success}{Whether or not the run was a success}
#'
#'
#'
#'
#'
#' @docType package
#' @name depwalker
#' @import data.table
NULL

# nocov start
.onLoad	<-	function(libname,	pkgname)	{
  op	<-	options()
  op.depwalker	<-	list(
    object.load.speed	=	630030164/100,
    object.save.extension	=	".rds",
    metadata.save.extension	=	".meta.yaml",
    lock.extension	=	'.lock',
    echo.extension	=	".log",
    runtime_objects_index.extension = '.rdx',
    error.extension	=	'.err.log',
    reserved_attr_for_hash ='..hash',
    default.lock.time = 3600, #1 hour
    tune.threshold_objsize_for_dedicated_archive = 5000 #Results from `studium_save`. It will be 4×this size for 'xz'.
  )
  toset	<-	!(names(op.depwalker)	%in%	names(op))
  if(any(toset))	options(op.depwalker[toset])
  invisible()
}
# nocov end
