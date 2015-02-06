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
#'  RepopulateStaticTables(config)
#'  }
NULL

#' @rdname StaticTables
PopulateStaticTables <- function(config) {
  conn <- StartDBConnection(config)
  
  cluster <- StartCluster(config, ncores = config$parallel$max.downloaders)
  StartClusterDBConnections(cluster = cluster, config = config)
  
  .PopulateParamCodes(conn = conn, config = config)
  .PopulateActiveSites(conn = conn, config = config)
  .PopulateSiteAssets(conn = conn, config = config)
  .PopulateSiteMetadata(conn = conn, config = config)
  .PopulateSensorMetadata(conn = conn, config = config)
  
  StopClusterDBConnections(cluster = cluster, config = config)
  StopCluster(cluster = cluster, config = config)
  StopDBConnection(conn = conn, config = config)
}

#' @rdname StaticTables
DropStaticTables <- function(config) {
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
WhichStaticTablesExist <- function(config) {
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
RepopulateStaticTables <- function(config) {
  DropStaticTables(config)
  PopulateStaticTables(config)
}

# @TODO Add a check on database after completion to make sure all 50 states are present. 
#' Populate the active.sites table specified in \code{config} using \code{conn}.
.PopulateActiveSites <- function(conn, config) {
  
  .message(paste("Populating table", 
                 config$tables$active.sites, 
                 "with active sites."), 
           config = config)
  
  .message(paste("Active sites are defined as sites with instantaneous values that have been updated within the lookback period: ",
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
  
  setTxtProgressBar(pb, 50)
  cat("\n")
  
  .message(paste("Table", 
                 config$tables$active.sites, 
                 "now populated with active sites."), 
           config = config)
}

#' Populate the site.assets table specified in \code{config} using \code{conn}.
.PopulateSiteAssets <- function(conn, config) {
  
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
      commaSeparated <- paste(map[[i]], collapse=',')
      .DownloadAssetsForSites(sites = commaSeparated, config = config)
    } else {
      .DownloadAssetsForSites(sites = map[[i]], config = config)
    }
  }
  
  cc <- foreach(i = 2:length(map)) %dopar% { 
    setTxtProgressBar(pb, i)
    result = tryCatch({
      if(length(map[[i]]) > 1){
        commaSeparated <- paste(map[[i]], collapse=',')
        .DownloadAssetsForSites(sites = commaSeparated, config = config)
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
  cat("\n")
  
  .message(paste("Table", 
                 config$tables$site.assets, 
                 "now populated with site invetories."), 
           config = config)
}

#' Populate the param.codes table specified in \code{config} using \code{conn}.
.PopulateParamCodes <- function(conn, config) {
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
.PopulateParamMetadata <- function(conn, config) {
  .message(paste("Populating table", 
                 config$tables$param.metadata, 
                 "with parameter metadata."), 
           config = config)
}

#' Populate the sensor.metadata table specified in \code{config} using \code{conn}.
.PopulateSensorMetadata <- function(conn, config) {
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
.PopulateSiteMetadata <- function(conn, config) {
  .message(paste("Populating table", 
                 config$tables$site.metadata, 
                 "with site metadata."), 
           config = config)
  
  query <- paste("SELECT DISTINCT
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
  b.state_cd,
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
  
  query <- paste("ALTER TABLE", config$tables$site.metadata,
                 "ADD COLUMN huc_l1 text,
                 ADD COLUMN huc_l2 text,
                 ADD COLUMN huc_l3 text;")
  
#                 ADD COLUMN huc_l4 text;")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <-paste("CREATE TABLE hucs_temp (huc_l1 text, 
                huc_l2 text, huc_l3 text, huc_l4 text);")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("INSERT INTO hucs_temp (huc_l1, huc_l2, huc_l3, huc_l4)
SELECT 
substring(huc_cd from 1 for 2),
substring(huc_cd from 1 for 4),
substring(huc_cd from 1 for 6),
substring(huc_cd from 1 for 8)
FROM ", config$tables$site.metadata, ";", sep = "")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE ", config$tables$site.metadata,
                 " set huc_l1 = hucs_temp.huc_l1,
huc_l2 = hucs_temp.huc_l2,
huc_l3 = hucs_temp.huc_l3
from hucs_temp
where ", config$tables$site.metadata, ".huc_cd = hucs_temp.huc_l4;",
                 sep = "")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("DROP TABLE hucs_temp;")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("ALTER TABLE", config$tables$site.metadata, "
ADD COLUMN nfie_hydro_region_num text, 
ADD COLUMN nfie_hydro_region_name text;")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '01',
nfie_hydro_region_name = 'New England'
where huc_l1 = '01';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '02',
nfie_hydro_region_name = 'Mid-Atlantic'
where huc_l1 = '02';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '03',
nfie_hydro_region_name = 'South Atlantic-Gulf'
where huc_l1 = '03';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '04',
nfie_hydro_region_name = 'Great Lake'
where huc_l1 = '04';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '08',
nfie_hydro_region_name = 'Mississippi'
where 
huc_l1 = '05' OR
huc_l1 = '06' OR
huc_l1 = '07' OR
huc_l1 = '08' OR
huc_l1 = '10' OR
huc_l1 = '11';
")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '09',
nfie_hydro_region_name = 'Souris-Red-Rainy'
where huc_l1 = '09';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '12',
nfie_hydro_region_name = 'Texas-Gulf'
where huc_l1 = '12';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '13',
nfie_hydro_region_name = 'Rio Grande'
where huc_l1 = '13';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '15',
nfie_hydro_region_name = 'Colorado'
where 
huc_l1 = '14' OR
huc_l1 = '15';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '16',
nfie_hydro_region_name = 'Great Basin'
where huc_l1 = '16';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '17',
nfie_hydro_region_name = 'Pacific Northwest'
where huc_l1 = '17';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  query <- paste("UPDATE", config$tables$site.metadata, "set 
nfie_hydro_region_num = '18',
nfie_hydro_region_name = 'California'
where huc_l1 = '18';")
  
  result <- RunQuery(conn = conn, 
                     query = query, 
                     config = config)
  
  
  .message(paste("Table", 
                 config$tables$site.metadata, 
                 "now populated with site metadata."), 
           config = config)
}
