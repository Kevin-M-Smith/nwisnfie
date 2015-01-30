BuildFileNamesAndLayerQueriesForAllSubsets <- function(suffix, config, conn) {
  
  HUCL1 <- .GetUniqueSubsets(config, subsetName = "huc_l1", conn = conn)
  HUCL2 <- .GetUniqueSubsets(config, subsetName = "huc_l2", conn = conn)
  HUCL3 <- .GetUniqueSubsets(config, subsetName = "huc_l3", conn = conn)
  HUCL4 <- .GetUniqueSubsets(config, subsetName = "huc_cd", conn = conn)
  NFIEHydro <- .GetUniqueSubsets(config, subsetName = "nfie_hydro_region_num", conn = conn)
  
  
  prefix <- paste0(config$netcdf$national, "/", suffix)
  
  dir.create(prefix, showWarnings = FALSE)
  
  
  
  NationalName <-   paste0(prefix,
                           "/",
                           "national",
                           "_",
                           suffix,
                           ".nc")
  
  HUCL1Names <- .BuildUniqueNames(config = config, 
                                  subset = HUCL1, 
                                  subsetName = "huc_l1",
                                  prefix = config$netcdf$huc_l1, 
                                  suffix = suffix)
  
  HUCL2Names <- .BuildUniqueNames(config = config, 
                                  subset = HUCL2, 
                                  subsetName = "huc_l2",
                                  prefix = config$netcdf$huc_l2, 
                                  suffix = suffix)
  
  HUCL3Names <- .BuildUniqueNames(config = config, 
                                  subset = HUCL3, 
                                  subsetName = "huc_l3",
                                  prefix = config$netcdf$huc_l3, 
                                  suffix = suffix)
  
  HUCL4Names <- .BuildUniqueNames(config = config, 
                                  subset = HUCL4,
                                  subsetName = "huc_l4",
                                  prefix = config$netcdf$huc_l4, 
                                  suffix = suffix)
  
  NFIEHydroNames <- .BuildUniqueNames(config = config, 
                                      subset = NFIEHydro, 
                                      subsetName = "nfie_hydro_region_num",
                                      prefix = config$netcdf$nfie_hydro,
                                      suffix = suffix)
  
  NationalSites <- .GetNationalSites(config = config, conn = conn)
  
  HUCL1Sites <- .GetSubsetSites(config = config,
                                subset = HUCL1,
                                subsetName = "huc_l1",
                                conn = conn)
  
  HUCL2Sites <- .GetSubsetSites(config = config,
                                subset = HUCL2,
                                subsetName = "huc_l2",
                                conn = conn)
  
  HUCL3Sites <- .GetSubsetSites(config = config,
                                subset = HUCL3,
                                subsetName = "huc_l3",
                                conn = conn)
  
  HUCL4Sites <- .GetSubsetSites(config = config,
                                subset = HUCL4,
                                subsetName = "huc_cd",
                                conn = conn)
  
  NFIEHydroSites <- .GetSubsetSites(config, 
                                    subset = NFIEHydro,
                                    subsetName = "nfie_hydro_region_num", 
                                    conn = conn)
  
  AllNames <- mapply(c, 
                     NationalName,
                     NFIEHydroNames,
                     HUCL1Names,
                     HUCL2Names,
                     HUCL3Names,
                      HUCL4Names,
                     SIMPLIFY = FALSE)
  
  
  AllSiteQueries <- mapply(c, 
                           NationalSites,
                           NFIEHydroSites,
                           HUCL1Sites,
                           HUCL2Sites,
                           HUCL3Sites,
                            HUCL4Sites,
                           SIMPLIFY = FALSE)
  
  data.frame(name = unlist(AllNames, recursive = FALSE), 
             query = unlist(AllSiteQueries, recursive = FALSE),
             stringsAsFactors = FALSE)
  
}

.BuildUniqueNames <- function(config, subsetName, subset, prefix, suffix) {
  
  NameBuilder <- function(sub) {
    
    prefix <- paste0(prefix, "/", suffix)
    
    dir.create(prefix, showWarnings = FALSE)
    
    paste0(prefix,
           "/",
           subsetName,
           "_",
           sub,
           "_",
           suffix,
           ".nc")
  }
  
  lapply(subset, NameBuilder)
  
}

.GetUniqueSubsets <- function(config, subsetName, conn) {
  
  query <- paste0("select distinct ", 
                  subsetName, 
                  " from ",
                  config$tables$site.metadata,
                  " where ",
                  subsetName,
                  " != '' ;")
  
  RunQuery(query = query,
           conn = conn,
           config = config)
  
}

.GetNationalSites <- function(config, conn) {
  query <- paste0("select familyid from ", 
                  config$tables$site.metadata,
                  ";")
  
  list(query)
}

.GetSubsetSites <- function(config, subset, subsetName, conn) {
  
  SubsetSites <- function(sub) {
    query <- paste0("select familyid from ", 
                    config$tables$site.metadata,
                    " where ",
                    subsetName,
                    " = '",
                    sub,
                    "'; ")
  }
  
  lapply(subset, SubsetSites)
}

