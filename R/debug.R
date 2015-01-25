DownloadOneSite <- function(config){
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
  cc <- foreach(i = 1) %dopar% { 
    .DownloadDataFromNWIS(site = sites[1,],
                          params = config$collections$params,
                          startDate = NULL,
                          endDate = NULL,
                          period = "P1W",
                          offset = NULL, 
                          config = config)
  }

  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}

Sample24H <- function(config) {
  
  conn <- StartDBConnection(config)
  
  query = "select * from data where ts > now() - interval '1 day';"
  
  data <- RunQuery(conn = conn, 
                   config = config,
                   query = query)
  
  StopDBConnection(conn = conn, config = config)
  
  return(data)
}

Upgrade <- function(){
  
  detach("package:nwisnfie", unload=TRUE)
  remove.packages("nwisnfie")
  
  library(devtools)
  install_github("Kevin-M-Smith/nwisnfie", ref = "swift")
  library(nwisnfie)
}