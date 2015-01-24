BootstrapSwift <- function(config) {
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
#  startDate <- lubridate::floor_date(now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P5W"), "day")
#  endDate   <- lubridate::floor_date(now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P1D"), "day")
#  dates     <- strftime(seq(startDate, endDate, by = "day"), '%Y-%m-%d')
 
dates <- c("a", "b")

#  pb <- txtProgressBar(min = 1, max = nrow(sites) * length(dates), style = 3, width = 20)
  
  cc <-  foreach(j = 1:length(dates)) %:% foreach(i = 1:nrow(sites)) %dopar% {
#        setTxtProgressBar(pb, i*j)
      
#         result = tryCatch({
#           .DownloadDataFromNWIS(site = sites[i,1],
#                                 params = config$collections$params,
#                                 startDate = dates[j],
#                                 endDate = dates[j],
#                                 period = NULL,
#                                 offset = config$time$midnight.offset.standard, 
#                                 stage = TRUE,
#                                 config = config)
#         
#         
#         }, warning = function(w) {
#         
#         }, error = function(e){
#           error <- paste("\nSite:",
#                          sites[i,1],
#                          "at index",
#                          i,
#                          "failed:",
#                          e)
#           .stop(error , config = config)
#       })
#         
    }
  
  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}