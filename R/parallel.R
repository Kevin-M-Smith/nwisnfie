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
#' \code{TestClusterSettings} runs \code{StartCluster} and \code{StopCluster} in succession. 
#' @examples
#' \dontrun{
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  cluster <- StartCluster(config)
#'  StopCluster(cluster = cluster, config = config)
#'  # or equivalently:
#'  TestClusterSettings(config)
#' }
NULL

#' @rdname Parallelization
StartCluster <- function(config){
  
  .message("Loading parallelization backend.", config = config)
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
  
  .message(paste("Attempting to start PSOCK cluster with", 
                 ncores, 
                 "workers."), 
           config = config)
  
  NewClusterMessage <- function(message, config){
    .message(paste("Successfully built a ",
                   capture.output(print(message)),
                   ".", 
                   sep = ""),
             config = config)
  }
  

  cluster <- parallel::makePSOCKcluster(names = ncores, outfile = "")
  
  doParallel::registerDoParallel(cluster)
  parallel::clusterExport(cluster, "config")
  
  lapply(cluster, NewClusterMessage, config)
  
  parallel::clusterEvalQ(cluster,{
    conn2 <- nwisnfie::StartDBConnection(config = config)
  })
    
  return(cluster)
}

#' @rdname Parallelization
StopCluster <- function(cluster, config){
  .message(paste("Attempting to stop ", 
                 capture.output(print(cluster)),
                 ".", 
                 sep = ""), 
           config = config)
  
  cc <- parallel::clusterEvalQ(cluster, {
    DBI::dbDisconnect(conn2)
  })
  
  cc <- parallel::stopCluster(cluster)
  
  .message("Cluster stopped successfully.", config = config)
}

#' @rdname Parallelization
TestClusterSettings <- function(config){
  cluster <- StartCluster(config)
  StopCluster(cluster = cluster, config = config)
}

