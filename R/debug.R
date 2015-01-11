SuperTest <- function(config){
  TestClusterSettings(config)
  RunDBDiagnostics(config)
  RebuildStaticTables(config)
}

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

.GetAllSites <- function(conn, config){
  query <- paste("SELECT site_no from ", config$tables$active.sites, ";", sep = "")
  sites <- RunQuery(conn = conn, query = query, config = config)
}

