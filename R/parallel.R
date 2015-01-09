# https://github.com/hadley/plyr/blob/80af6d9f1b53e01222edfb6db50dc83a4624f2c4/R/llply.r


#' Cluster Management
#' 
#' Functions for cleanly starting and stopping PSOCK clusters. 
#' These clusters are used by \code{nwisnfie} to perform embarassingly parallel tasks in tandem.
#' PSOCK clusters are used for maximum compatibility across platforms. 
#'
#' @name Cluster Management
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.




#' Starts Cluster for Parallel Operations
#' 
#' This function starts up the cluster, loads the \code{nwisnfie} library, and starts database connections
#'
#'
.StartupCluster <- function(config){
  cluster <- parallel::makePSOCKcluster(
    names = parallel::detectCores(), 
    outfile = "")
  doParallel::registerDoParallel(cluster)
  parallel::clusterExport(cluster, "config")
  parallel::clusterEvalQ(cluster,{
    #library(doParallel)
    conn2 <- nwisnfie::StartDBConnection(config = config)
  })
}

