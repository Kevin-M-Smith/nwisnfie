SyncDB <- function(config, period){
  
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
  # map.size controls how many sites are downloaded in a single REST call
  map.size = 100
  map <- unlist(lapply(sites, as.character))
  map <- split(map, ceiling(seq_along(map)/map.size))
  
  pb <- txtProgressBar(min = 1, max = length(map), style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% { 
    
    if(length(map[[i]]) > 1){
      commaSeparated <- paste(map[[i]], collapse=',')
      .DownloadDataFromNWIS(site = commaSeparated,
                            params = config$collections$params,
                            startDate = NULL,
                            endDate = NULL,
                            period = period,
                            offset = NULL, 
                            config = config)
    } else {
      .DownloadDataFromNWIS(site = map[[i]],
                            params = config$collections$params,
                            startDate = NULL,
                            endDate = NULL,
                            period = period,
                            offset = NULL, 
                            config = config)
    }
  }
  
  cc <- foreach(i = 2:length(map)) %dopar% {
    setTxtProgressBar(pb, i)
    
    result = tryCatch({
      if(length(map[[i]]) > 1){
        
        commaSeparated <- paste(map[[i]], collapse=',')
        .DownloadDataFromNWIS(site = commaSeparated,
                              params = config$collections$params,
                              startDate = NULL,
                              endDate = NULL,
                              period = period,
                              offset = NULL, 
                              config = config)
      } else {
        .DownloadDataFromNWIS(site = map[[i]],
                              params = config$collections$params,
                              startDate = NULL,
                              endDate = NULL,
                              period = period,
                              offset = NULL, 
                              config = config)
      }
      
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