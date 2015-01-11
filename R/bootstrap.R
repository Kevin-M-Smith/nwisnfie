Bootstrap <- function(config) {
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
  pb <- txtProgressBar(min = 1, max = nrow(sites), style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% { 
    .DownloadDataFromNWIS(site = sites[i,],
                          params = config$collections$params,
                          startDate = NULL,
                          endDate = NULL,
                          period = config$collections$lookback,
                          offset = NULL, 
                          config = config)
  }
  
  cc <- foreach(i = 2:nrow(sites)) %dopar% {
    setTxtProgressBar(pb, i)
    
    result = tryCatch({
      .DownloadDataFromNWIS(site = sites[i,1],
                          params = config$collections$params,
                          startDate = NULL,
                          endDate = NULL,
                          period = config$collections$lookback,
                          offset = NULL, 
                          config = config)
    }, warning = function(w) {
      
    }, error = function(e){
      error <- paste("\nSite:",
                     sites[i,1],
                     "at index",
                     i,
                     "failed:",
                     e)
      .stop(error , config = config)
    })
    
  }
  
  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}