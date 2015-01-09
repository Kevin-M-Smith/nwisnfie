#' Populates static tables in database. 
#' 
#' The following tables are considered 'static':
#' \enumerate{
#' \item param.codes
#' \item site.metadata
#' \item param.metadata
#' \item active.sites
#' \item site.assets
#' }
#' 
#' @param config A configuration object created by LoadConfiguration
#' @return NULL
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
PopulateStaticTables <- function(config){
  conn <- StartDBConnection(config)
  
  .StartupCluster(config)
  
  .PopulateParamCodes(conn = conn, config = config)
  .PopulateActiveSites(conn = conn, config = config)
}

#' Populate the param.codes table specified in \code{config}.
.PopulateParamCodes <-function(conn, config){
  RPostgreSQL::dbWriteTable(conn = conn, 
                            name = config$tables$param.codes, 
                            parameter_codes)
}

#' Populate the active.sites table specified in \code{config}.
.PopulateActiveSites <- function(conn, config){
  pb <- txtProgressBar(min = 1, max = 49, style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% { 
    .DownloadActiveSitesForState(state.abb[i]) 
  }
  
  cc <- foreach(i = 2:50) %dopar% {
    setTxtProgressBar(pb, i)
    .DownloadActiveSitesForState(state.abb[i]) 
  }
  
}

