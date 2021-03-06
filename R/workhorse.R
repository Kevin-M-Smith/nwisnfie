DownloadAndBuildDay <- function(config, date) {
  
  ##############################
  #      SETUP CONNECTION
  ##############################
  conn <- StartDBConnection(config)
  cluster <- StartCluster(config, ncores = config$parallel$max.downloaders)
  StartClusterDBConnections(cluster = cluster, config = config)
  
  ##############################
  #      GET SITES
  ##############################
  allSites <- .GetAllSites(conn = conn, config = config)
  
  ##############################
  #      SETUP TABLE
  ##############################
  tableName <- paste0(config$tables$staging.prefix, 
                      gsub(pattern = "-", replacement = "_", date))
  
  .CreateDataTable(conn = conn, config = config, tableName = tableName)
  .CreateDataTableUpsertTrigger(conn = conn, config = config, tableName = tableName)
  DisableAutovacuum(table = tableName, config = config)    
  
  #####################################
  #      SPLIT SITES FOR DOWNLOAD
  #####################################
  #  mapSize controls how many sites are downloaded in a single REST call
  mapSize = 100
  map <- unlist(lapply(allSites, as.character))
  map <- split(map, ceiling(seq_along(map)/mapSize))
  
  .message(paste0("Downloading data for ", date, "..."), config = config)
  
  pb <- txtProgressBar(min = 1, max = length(map), style = 3, width = 20)
  
  #####################################
  #      Download
  #####################################
  cc <- foreach(i = 1:length(map)) %dopar% {
    setTxtProgressBar(pb, i)
    
    if(length(map[[i]]) > 1){
      commaSeparated <- paste(map[[i]], collapse=',')
      .DownloadDataFromNWIS(sites = map[[i]],
                            params = unlist(strsplit(config$collections$params, split = ',')),
                            startDate = date,
                            endDate = date,
                            offset = config$time$utc.offset, 
                            tableName = tableName,
                            config = config)
    } else {
      .DownloadDataFromNWIS(sites = map[[i]],
                            params = unlist(strsplit(config$collections$params, split = ',')),
                            startDate = date,
                            endDate = date,
                            offset = config$time$utc.offset, 
                            tableName = tableName,
                            config = config)
    }
  }
  cat("\n")
  
  StopClusterDBConnections(cluster = cluster, config = config)
  StopCluster(cluster = cluster, config = config)
  
  #####################################
  #      Prepare Bulk NetCDF Build
  #####################################
  cluster <- StartCluster(config, ncores = config$parallel$max.builders)
  StartClusterDBConnections(cluster = cluster, config = config)
  
  .message(paste0("Building NetCDF Files for ", date, "..."), config = config)
  
  query <- paste0("select ts, familyid, value, paramcd, validated from \"", 
                  tableName,
                  "\";")
  
  data <- RunQuery(conn = conn,
                   query = query,
                   config = config)
  
  #####################################
  #     Build All Subsets
  #####################################
  
  queue <- BuildSubsetQueue(suffix = date, config = config, conn = conn)
  BuildNetCDF(data = data, queue = queue, cluster = cluster, suffix = date, config = config, conn = conn)
  
  #   .DropDataTableUpsertTrigger(conn = conn, config = config, tableName = tableName)
  #   .DropDataTable(conn = conn, config = config, tableName = tableName)
  
  StopClusterDBConnections(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
  StopCluster(cluster = cluster, config = config)
  
}



