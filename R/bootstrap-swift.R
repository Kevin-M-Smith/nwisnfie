BootstrapSwift <- function(config) {
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
  startDate <- lubridate::floor_date(lubridate::now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P5W"), "day")
  endDate   <- lubridate::floor_date(lubridate::now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P1D"), "day")
  dates     <- strftime(seq(startDate, endDate, by = "day"), '%Y-%m-%d')
  
  pb <- txtProgressBar(min = 1, max = nrow(sites), style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% {
    setTxtProgressBar(pb, i)
    
    DownloadWrapper <- function(date) {
      
      .DownloadDataFromNWIS(site = sites[i,1],
                            params = config$collections$params,
                            startDate = date,
                            endDate = date,
                            period = NULL,
                            offset = config$time$utc.offset, 
                            stage = TRUE,
                            config = config)
      
      table <- paste0(config$tables$staging, date)
            
      DisableAutovacuum(table = table, config = config)
      
    }
    
    lapply(dates, DownloadWrapper)
  } 
  
  #cc <- foreach(i = 2:nrow(sites)) %dopar% {
  cc <- foreach(i = 2:10) %dopar% { 
    setTxtProgressBar(pb, i)
    
    DownloadWrapper <- function(date) {
      
      .DownloadDataFromNWIS(site = sites[i,1],
                            params = config$collections$params,
                            startDate = date,
                            endDate = date,
                            period = NULL,
                            offset = config$time$utc.offset, 
                            stage = TRUE,
                            config = config)
      
    }
    
    lapply(dates, DownloadWrapper)
  }
  
  cc <- foreach(i = 1:length(dates)) %dopar% {
    
    query <- paste0("select * from \"", 
                    paste0(config$tables$staging, 
                           dates[i]),
                    "\"",
                    ";")
    
    data <- RunQuery(conn = conn2,
             query = query,
             config = config)
    
    print(head(data))
  }
  
  
  
  
  
  
  
  
  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}