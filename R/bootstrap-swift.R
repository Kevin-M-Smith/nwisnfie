BootstrapSwift <- function(config) {
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
 # sites <- .GetAllSites(conn = conn, config = config)
  
  startDate <- lubridate::floor_date(lubridate::now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P5W"), "day")
  endDate   <- lubridate::floor_date(lubridate::now(tz="utc") + .ParseISO8601Offset("-06:00") - .ParseISO8601Duration("P1D"), "day")
  dates     <- strftime(seq(startDate, endDate, by = "day"), '%Y-%m-%d')
  
#  pb <- txtProgressBar(min = 1, max = nrow(sites), style = 3, width = 20)
  
#   cc <- foreach(i = 1) %dopar% {
#     setTxtProgressBar(pb, i)
#     
#     DownloadWrapper <- function(date) {
#       
#       .DownloadDataFromNWIS(site = sites[i,1],
#                             params = config$collections$params,
#                             startDate = date,
#                             endDate = date,
#                             period = NULL,
#                             offset = config$time$utc.offset, 
#                             stage = TRUE,
#                             config = config)
#       
#       table <- paste0(config$tables$staging, date)
#             
#       DisableAutovacuum(table = table, config = config)
#       
#     }
#     
#     lapply(dates, DownloadWrapper)
#   } 
#   
#   #cc <- foreach(i = 2:nrow(sites)) %dopar% {
#   cc <- foreach(i = 2:10) %dopar% { 
#     setTxtProgressBar(pb, i)
#     
#     DownloadWrapper <- function(date) {
#       
#       .DownloadDataFromNWIS(site = sites[i,1],
#                             params = config$collections$params,
#                             startDate = date,
#                             endDate = date,
#                             period = NULL,
#                             offset = config$time$utc.offset, 
#                             stage = TRUE,
#                             config = config)
#       
#     }
#     
#     lapply(dates, DownloadWrapper)
#   }
#   
  
  stagingTable <- "staging_2014-12-20"
  
  
#   cc <- foreach(i = 1:length(dates)) %dopar% {
#     
#     stagingTable <- paste0(config$tables$staging, dates[i])
    
    query <- paste0("select ts, familyid, value, paramcd, validated from \"", 
                    stagingTable,
                    "\"",
                    ";")
    
    data <- RunQuery(conn = conn,
             query = query,
             config = config)
    
#     .message(head(data), config = config)
#     
#   }
  
  queue <- BuildFileNamesAndLayerQueriesForAllSubsets(suffix = "2014-12-20", config = config, conn = conn)
  

  cc <- foreach(i = 1:10) %dopar% {
    familyids <- RunQuery(conn = conn2,
                          query = queue$query[i],
                          config = config)
    
    sub <- subset(data, subset = familyid %in% familyids[,1])
    
    .message(head(sub), config = config)
  #  BuildNetCDF(data = sub, name = queue$name[i], config, conn = conn2)
  }
  
  
  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}