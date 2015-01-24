BootstrapSwift <- function(config) {
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
 startDate <- lubridate::floor_date(now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P5W"), "day")
 endDate   <- lubridate::floor_date(now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P1D"), "day")
 dates     <- strftime(seq(startDate, endDate, by = "day"), '%Y-%m-%d')
 
  pb <- txtProgressBar(min = 1, max = nrow(sites), style = 3, width = 20)
  
  cc <- foreach(i = 1:nrow(sites)) %dopar% {
       setTxtProgressBar(pb, i)
       
       DownloadWrapper <- function(date) {
      
        .DownloadDataFromNWIS(site = sites[i,1],
                                params = config$collections$params,
                                startDate = date,
                                endDate = date,
                                period = NULL,
                                offset = config$time$midnight.offset.standard, 
                                stage = TRUE,
                                config = config)
          
       }
       
       lapply(dates, DownloadWrapper)
    }
  
  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}