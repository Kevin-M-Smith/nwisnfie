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
               "&period=",
               config$collections$lookback,
               "&siteOutput=expanded&hasDataTypeCd=iv,id",
               sep = "")
  
  active <- dataRetrieval::importRDB1(url)
  RPostgreSQL::dbWriteTable(conn = conn2, 
                            name = config$tables$active.sites, 
                            value = active, 
                            append = TRUE, 
                            row.names = FALSE, 
                            overwrite = FALSE)  
}

#' Downloads Assets at Sites
#' 
#' Downloads & imports the inventory of instantaneous values at a given set of sites. 
#' 
#' @param sites A character vector of USGS site numbers, comma separated, with no spaces. (e.g. "0101010,1020202,10101010")
#' @param config Configuration object created by LoadConfiguration.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
.DownloadAssetsForSites <- function(sites, config){
  
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb,1.0&sites=",
               sites,
               "&seriesCatalogOutput=true&outputDataTypeCd=iv",
               sep = "")
  
  assets <- dataRetrieval::importRDB1(url)
  assets <- base::transform(assets, 
                      seriesid = paste(agency_cd, ":", site_no, ":", parm_cd, ":00011:", 
                                       formatC(dd_nu, width = 5, format = "d", flag = "0"), sep = ""),
                      familyid = paste(agency_cd, ":", site_no, ":00011:", 
                                       formatC(dd_nu, width = 5, format = "d", flag = "0"), sep = ""))
  
  if(nrow(assets) > 0){
    RPostgreSQL::dbWriteTable(conn = conn2, 
                              name = config$tables$site.assets, 
                              value = assets, 
                              append = TRUE, 
                              row.names = FALSE, 
                              overwrite = FALSE)
  }
}

