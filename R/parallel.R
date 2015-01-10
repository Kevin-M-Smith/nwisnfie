#' Cluster Management
#' 
#' Functions for cleanly starting and stopping PSOCK clusters. 
#' These clusters are used by \code{nwisnfie} to perform embarassingly parallel tasks in tandem.
#' PSOCK clusters are used for maximum compatibility across platforms. 
#'
#' @name Parallelization
#' @param config Configuration object created by \code{LoadConfiguration}
#' @param cluster A cluster reference produced by \code{StartCluster}
#' @return \code{StartCluster} returns an object of class c("SOCKcluster", "cluster"), as built by \code{parallel::makePSOCKcluster}.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @details
#' \code{StartCluster} starts up the cluster, loads the \code{nwisnfie} library, and starts individual database connections. 
#' \code{StopCluster} closes out all database connections, and destroys the cluster.
NULL

#' @rdname Parallelization
StartCluster <- function(config){
  
  cat("Loading parallelization backend. \n")
  library(foreach)
  library(doParallel)
    
  if (config$parallel$max.cores == "NONE"){
    ncores = parallel::detectCores()
  } else if (is.numeric(config$parallel$max.cores)){
    if (config$parallel$max.cores > parallel::detectCores()){
      ncores = parallel::detectCores()
    } else {
      ncores = config$parallel$max.cores
    }
  } else {
    warning("
            Did not recognize configuration for parallel/max.cores. 
            Defaulting to single core operations. 
            
            Please specify maximum number of cores numerically (e.g. 4) 
              or NONE to use all available cores.

            EXAMPLES
            parallel:
             max.cores: NONE

            parallel:
             max.cores: 2")
  }
  
  cat(paste("Attempting to start PSOCK cluster with", ncores, "workers. \n"))

  cluster <- parallel::makePSOCKcluster(
    names = ncores, 
    outfile = "")
  
  doParallel::registerDoParallel(cluster)
  parallel::clusterExport(cluster, "config")
  
  parallel::clusterEvalQ(cluster,{
    conn2 <- nwisnfie::StartDBConnection(config = config)
  })
    
  return(cluster)
}

#' @rdname Parallelization
StopCluster <- function(cluster){
  cat("Attempting to stop cluster. \n")
  
  cc <- parallel::clusterEvalQ(cluster, {
    DBI::dbDisconnect(conn2)
  })
  
  cc <- parallel::stopCluster(cluster)
  
  cat("Cluster stopped successfully. \n")
  
}



