# https://github.com/hadley/plyr/blob/80af6d9f1b53e01222edfb6db50dc83a4624f2c4/R/llply.r

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

