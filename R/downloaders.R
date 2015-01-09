#' 
#'
#' 
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
.DownloadActiveSitesForState <- function(state, config){
  #print(paste("Collecting active sites from ", state, "...", sep = ""))
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=",
               state,
               "&period=P5W&siteOutput=expanded&hasDataTypeCd=iv,id",
               sep = "")
  
  active <- dataRetrieval::importRDB1(url)
  RPostgreSQL::dbWriteTable(conn2, config$tables$active.sites, active, append = TRUE, row.names = FALSE, overwrite = FALSE)
  
  #pathToFile <- tempfile()
  #download.file(url, pathToFile)
  #active <- importRDB(pathToFile)
}
