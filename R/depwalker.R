#' depwalker: A dependency walker, that manage execution of user scripts.
#'
#'Depwalker tracks dependencies between the scripts trying to satisfy them in parallel,
#'caches the results for later fast retrieval, gives detailed execution log and performance metrics.
#'
#' @section Rationale
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
#' @section User scripts
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
#' @section Optimizations
#' Scripts are not executed, if there exists current valid cached version of the objects they produce, and that
#' reading the cache is actually faster, than execution the script (which may not be the case, if the script
#' reads the database from external source, or creates a lot of data in short time).
#'
#' If a script has more than one dependency, all of them are executed in parallel (or at least as many, as memory allows)
#'
#' After the object is created via execution of user script, it gets cached - i.e. saved on disk, to speed up
#' subsequent computations. Saving a large object can be time consuming, so by default it is done in parallel.
#'
NULL

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
