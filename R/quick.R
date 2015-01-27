quick <- function(config, date = "2014-12-21") {
  
  ##############################
  #      SETUP CONNECTION
  ##############################
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config)
  
  ##############################
  #      GET SITES
  ##############################
  sites <- .GetAllSites(conn = conn, config = config)
  
  ##############################
  #      SETUP TABLE
  ##############################
  tableName <- paste0(config$tables$staging.prefix, 
                      gsub(pattern = "-", replacement = "_", date))
  
  .CreateDataTable(conn = conn, config = config, tableName = tableName)
  .CreateDataTableUpsertTrigger(conn = conn, config = config, tableName = tableName)
  DisableAutoVacuum(table = tableName, config = config)
  
  ##############################
  #      SPLIT SITES INTO MAP
  ##############################
  # map.size controls how many sites are downloaded in a single REST call
  map.size = 100
  map <- unlist(lapply(sites, as.character))
  map <- split(map, ceiling(seq_along(map)/map.size))
  
  .message(paste0("Downloading data for ", date, "..."), config = config)
  
  pb <- txtProgressBar(min = 1, max = length(map), style = 3, width = 20)
  
  cc <- foreach(i = 1:length(map)) %dopar% {
    setTxtProgressBar(pb, i)
    
    if(length(map[[i]]) > 1){
      commaSeparated <- paste(map[[i]], collapse=',')
      .DownloadDataFromNWIS(site = commaSeparated,
                            params = config$collections$params,
                            startDate = date,
                            endDate = date,
                            period = NULL,
                            offset = config$time$utc.offset, 
                            stage = TRUE,
                            config = config)
    } else {
      .DownloadDataFromNWIS(site = map[[i]],
                            params = config$collections$params,
                            startDate = date,
                            endDate = date,
                            period = NULL,
                            offset = config$time$utc.offset, 
                            stage = TRUE,
                            config = config)
    }
  }

  .message(paste0("Building NetCDF Files for ", date, "..."), config = config)

  query <- paste0("select ts, familyid, value, paramcd, validated from \"", 
                tableName,
                "\;")

  data <- RunQuery(conn = conn,
                 query = query,
                 config = config)

  queue <- BuildFileNamesAndLayerQueriesForAllSubsets(suffix = date, config = config, conn = conn)

  cc <- foreach(i = 1) %dopar% {
    
    familyids <- RunQuery(conn = conn2,
                          query = queue$query[i],
                          config = config)
    
    sub <- subset(data, subset = familyid %in% familyids[,1])
    
    BuildNetCDF(data = sub, name = queue$name[i], config, conn = conn2)
  
  }


   .DropDataTableUpsertTrigger(conn = conn, config = config, tableName = tableName)
   .DropDataTable(conn = conn, config = config, tableName = tableName)
}



