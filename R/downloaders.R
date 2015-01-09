DownloadActiveSitesForState <- function(state){
  #print(paste("Collecting active sites from ", state, "...", sep = ""))
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=",
               state,
               "&period=P5W&siteOutput=expanded&hasDataTypeCd=iv,id",
               sep = "")
  
  active <- dataRetrieval::importRDB1(url)
  RPostgreSQL::dbWriteTable(conn2, "activesites", active, append = TRUE, row.names = FALSE, overwrite = FALSE)
  
  #pathToFile <- tempfile()
  #download.file(url, pathToFile)
  #active <- importRDB(pathToFile)
}
