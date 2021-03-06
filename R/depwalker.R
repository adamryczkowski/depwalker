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
#' \item{\strong{codepath}}{path to the R file with the main script. This path and all others can be absolute and can be
#'       relative to the location of this file itself. The contents of the script is part of the task definition.}
#' \item{\strong{extrasources}}{Optional dictionary with all the external files managed by the main R script.
#'       These may be source files
#'       in other languages like C++, or CSV files with data. Dictionary key is case-sensitive base file name.
#'       These source files (written in any language) \emph{are expected to be invoked from within the
#'       main script file (\code{codepath})} and will not be called automatically.
#'       There are tracked for changes, and if any of the secondary source files changes,
#'       the whole task will need to be re-run. }
#' \item{codeCRC}{MD5 hash of all the source files - \code{codepath} and all files in \code{extrasources}, in
#'      cannonical order.}
#' \item{\strong{parents}}{Dictionary with all parent task definitions. Dictionary key is base name of the task. }
#' \item{\strong{objectrecords}}{Dictionary with registered objects produced by the main R script.
#'       Dictionary key is the object name. }
#' \item{\strong{execution.directory}}{String with the directory when the script should be run. Can be NULL (or absent),
#'       or string with the directory. Directory can be absolute, or relative to the task's directory.
#'       e.g. execution.directory equal '' will mean the task's main directory.}
#' \item{flag.never.execute.parallel}{Boolean flag. If set, this task will never be executed in parallel to the main
#'       R task.}
#' \item{flag.force.recalculation}{Boolean flag. If set, next time the task will be executed regardless whether
#'       there is update version in the cache. After recalculation this flag will be cleared automatically.}
#' \item{timecosts}{List with run statistics of this task.}
#' }
#'
#' \strong{Description of the \code{extrasources} item}:
#' \describe{
#' \item{\strong{path}}{Path to the file. Can be absoulute or relative to the task metadata file.}
#' \item{flag.checksum}{Flag. If set, contents of the file will be checksummed and on any change of the contents, the task
#'       will be reexecuted. If cleared, only change of the filename will be considered when checking whether the cached
#'       task results are valid.}
#' \item{flag.binary}{Flag. If set, contents of the file will be treated as binary and checksummed as such. Otherwise they will be
#'       checkusmmed like source files, line-by-line to avoid Linux/Mac/Windows end-of-line character confussion.}
#' \item{flag.r}{Flag. If set, contents of the file will be treated as R source and formatted in the logs accordingly.}
#' }
#'
#' \strong{Description of the \code{parentrecord} item}:
#' \describe{
#' \item{\strong{path}}{Path (either absoulute or relative) to the imported task's metadata file.}
#' \item{\strong{name}}{Name of the imported object.}
#' \item{\strong{aliasname}}{Alternate name of the object, set if our script needs the object produced
#'       by the imported task in different name.}
#' }
#'
#' \strong{Description of the \code{objectrecord} item}:
#' \describe{
#' \item{\strong{name}}{Name of object produced by the main R script.}
#' \item{path}{Path to the location, where the cached version of the object should be saved after execution of the scirpt.
#'       For large objects it can point to the alternate storage or outside of the
#'       version-controlled/synchronized directory}
#' \item{compress}{Compression method. Currently the only methods supported are:
#'       \code{xz}, \code{bzip2}, \code{gzip} and \code{false}}
#' \item{objectdigest}{MD5 digest of this object when in R memory.}
#' \item{filedigest}{MD5 digest of the compressed file with the saved object.}
#' \item{size}{Size of the objects in bytes when in R memory. Usefull for memory usage optimization. }
#' \item{filesize}{Size of the compressed file with the saved object. When size mismatches, there is no need to check for
#'       MD5 hash.}
#' \item{mtime}{Modification time of the compressed file with the saved object. When \code{mtime} mismatches, there is
#'       no need to check for MD5 has.}
#'}
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
    error.extension	=	'.err.log'
  )
  toset	<-	!(names(op.depwalker)	%in%	names(op))
  if(any(toset))	options(op.depwalker[toset])
  invisible()
}
# nocov end
