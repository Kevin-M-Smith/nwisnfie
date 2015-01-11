#' Manage static tables. 
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
#' @name StaticTables
#' @param config A configuration object created by LoadConfiguration
#' @return NULL
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @examples
#' \dontrun{
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  PopulateStaticTables(config)
#'  DropStaticTables(config)
#'  # Equivalently...
#'  RebuildStaticTables(config)
#'  }
NULL

#' @rdname StaticTables
PopulateStaticTables <- function(config){
  conn <- StartDBConnection(config)
  
  cluster <- StartCluster(config)
  
  .PopulateParamCodes(conn = conn, config = config)
  .PopulateActiveSites(conn = conn, config = config)
  .PopulateSiteAssets(conn = conn, config = config)
  .PopulateSiteMetadata(conn = conn, config = config)
  .PopulateSensorMetadata(conn = conn, config = config)
  
  StopCluster(cluster = cluster, config = config)
}

#' @rdname StaticTables
DropStaticTables <- function(config){
  conn <- StartDBConnection(config)
  
  exists <- .WhichTablesExist(conn, config, quietly = TRUE)
  
  BuildAndRunQuery <- function(table){
    if (exists[table]) {

      .message(paste("Table '", 
                     config$tables[table],
                     "' (",
                     table,
                     ") exists.", 
                     sep = ""), 
               config = config)
      
      result <- RunQuery(conn = conn, 
                         query = paste("drop table ", 
                                       config$tables[table],
                                       ";", 
                                       sep = ""),
                         config = config)
      
      return(TRUE)
    } else {
      .message(paste("Table '", 
                     config$tables[table], 
                     "' (",
                     table,
                     ") does not exist.", 
                     sep = ""), 
               config = config)
      
      return(FALSE)
    }
  }
  
  staticTables <- list("param.codes",
                       "site.metadata",
                       "param.metadata",
                       "sensor.metadata",
                       "active.sites",
                       "site.assets")
  
  cc <- lapply(staticTables, BuildAndRunQuery)
  
  .message("No static tables remain.", config = config)
  
  cc <- StopDBConnection(conn = conn, config = config)
  
}

#' @rdname StaticTables
WhichStaticTablesExist <- function(config){
  .message("Checking existance of static tables...", config = config)
  conn <- StartDBConnection(config)
  
  exists <- .WhichTablesExist(conn, config, quietly = TRUE)

  CheckIfStaticTableExists <- function(table){
    if (exists[table]) {
      .message(paste("Table '", 
                     config$tables[table],
                     "' (",
                     table,
                     ") exists.", 
                     sep = ""), 
               config = config)
      
      return(TRUE)
    } else {
      .message(paste("Table '", 
                     config$tables[table], 
                     "' (",
                     table,
                     ") does not exist.", 
                     sep = ""), 
               config = config)
      
      return(FALSE)
    }
  }
  
  staticTables <- list("param.codes",
                       "site.metadata",
                       "param.metadata",
                       "sensor.metadata",
                       "active.sites",
                       "site.assets")
  
  cc <- lapply(staticTables, CheckIfStaticTableExists)
  cc <- StopDBConnection(conn = conn, config = config)
  
}

#' @rdname StaticTables
RebuildStaticTables <- function(config){
  DropStaticTables(config)
  PopulateStaticTables(config)
}

# @TODO Add a check on database after completion to make sure all 50 states are present. 
#' Populate the active.sites table specified in \code{config}.
.PopulateActiveSites <- function(conn, config){
  
  .message(paste("Populating table", 
                 config$tables$active.sites, 
                 "with active sites."), 
           config = config)
  
  .message(paste("Active sites are defined as sites with instantanous values that have
                 been updated within the lookback period: ",
                 config$collections$lookback, ".", sep = ""), 
           config = config)
  
  pb <- txtProgressBar(min = 1, max = 50, style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% { 
    .DownloadActiveSitesForState(state = state.abb[i], config = config) 
  }
  
  cc <- foreach(i = 2:50) %dopar% {
    setTxtProgressBar(pb, i)
    .DownloadActiveSitesForState(state = state.abb[i], config = config) 
  }
  
  setTxtProgressBar(pb, 50); 
  
  .message(paste("Table", 
                 config$tables$active.sites, 
                 "now populated with active sites."), 
           config = config)
}

#' Populate the site.assets table specified in \code{config}.
.PopulateSiteAssets <- function(conn, config){
  
  .message(paste("Populating table", 
                 config$tables$site.assets, 
                 "with an inventory of assets at each site."), 
           config = config)
  
  sites <- .GetAllSites(conn = conn, config = config)
  
  # map.size controls how many sites are downloaded in a single REST call
  map.size = 50
  map <- unlist(lapply(sites, as.character))
  map <- split(map, ceiling(seq_along(map)/map.size))
  
  pb <- txtProgressBar(min = 1, max = length(map), style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% { 
    if(length(map[[i]]) > 1){
      commaSeperated <- paste(map[[i]], collapse=',')
      .DownloadAssetsForSites(sites = commaSeperated, config = config)
    } else {
      .DownloadAssetsForSites(sites = map[[i]], config = config)
    }
  }
  
  cc <- foreach(i = 2:length(map)) %dopar% { 
    setTxtProgressBar(pb, i)
    result = tryCatch({
      if(length(map[[i]]) > 1){
        commaSeperated <- paste(map[[i]], collapse=',')
        .DownloadAssetsForSites(sites = commaSeperated, config = config)
      } else {
        .DownloadAssetsForSites(sites = map[[i]], config = config)
      } 
    }, warning = function(w) {
    }, error = function(e) {
      .warning(paste("Site:",
                     sites[i,1],
                     "at index",
                     i,
                     "failed:",
                     e))
    })
  }
  
  setTxtProgressBar(pb, length(map))
  
  .message(paste("Table", 
                 config$tables$site.assets, 
                 "now populated with site invetories."), 
           config = config)
}

#' Populate the param.codes table specified in \code{config}.
.PopulateParamCodes <- function(conn, config){
  .message(paste("Populating table", 
                 config$tables$param.codes, 
                 "with parameter codes."), 
           config = config)
  
  RPostgreSQL::dbWriteTable(conn = conn, 
                            name = config$tables$param.codes, 
                            parameter_codes)
  
  .message(paste("Table", 
                 config$tables$param.codes, 
                 "now populated with parameter codes."), 
           config = config)
}

# @TODO Not implemented. No parameter metadata available yet.
#' Populate the param.metadata table specified in \code{config}.
.PopulateParamMetadata <- function(conn, config){
  .message(paste("Populating table", 
                 config$tables$param.metadata, 
                 "with parameter metadata."), 
           config = config)
}

#' Populate the sensor.metadata table specified in \code{config}.
.PopulateSensorMetadata <- function(conn, config){
  .message(paste("Populating table", 
                 config$tables$sensor.metadata, 
                 "with sensor metadata."), 
           config = config)
  
  query = paste("SELECT
  a.familyid,
  a.parm_cd,
  a.loc_web_ds
INTO ", config$tables$sensor.metadata, "
FROM 
  public.", config$tables$site.assets, " a
LEFT OUTER JOIN 
  public.", config$tables$active.sites, " b
ON
  (a.site_no = b.site_no);",
                sep = "")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  .message(paste("Table", 
                 config$tables$sensor.metadata, 
                 "now populated with sensor metadata."), 
           config = config)
}

#' Populate the site.metadata table specified in \code{config}.
.PopulateSiteMetadata <- function(conn, config){
  .message(paste("Populating table", 
                 config$tables$site.metadata, 
                 "with site metadata."), 
           config = config)
  
  query = paste("SELECT DISTINCT
  a.familyid,
  a.site_no,
  a.dd_nu,
  a.station_nm,
  a.site_tp_cd,
  a.dec_lat_va,
  a.dec_long_va,
  a.dec_coord_datum_cd,
  a.alt_va,
  a.alt_datum_cd,
  a.huc_cd,
  b.tz_cd,
  b.agency_cd,
  b.district_cd,
  b.county_cd,
  b.country_cd
INTO ", config$tables$site.metadata, 
                " FROM public.", config$tables$site.assets, " a 
LEFT OUTER JOIN 
   public.", config$tables$active.sites, " b 
ON
  (a.site_no = b.site_no);", sep = "")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  .message(paste("Table", 
                 config$tables$site.metadata, 
                 "now populated with site metadata."), 
           config = config)
}






