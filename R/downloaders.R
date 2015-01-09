#' Download Active Sites for State
#' 
#' Downloads the 'Active Sites' in a given state.
#' 
#' @param state A two character code specifying a state. (e.g. "AK" for Alaska)
#' @param config Configuration object created by LoadConfiguration.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @examples
#' # Alphabetically, Alaska is the second state...
#' print(state.abb[2])
#'
.DownloadActiveSitesForState <- function(state, config){
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=",
               state,
               "&period=P5W&siteOutput=expanded&hasDataTypeCd=iv,id",
               sep = "")
  
  active <- dataRetrieval::importRDB1(url)
  RPostgreSQL::dbWriteTable(conn2, config$tables$active.sites, active, append = TRUE, row.names = FALSE, overwrite = FALSE)  
}
