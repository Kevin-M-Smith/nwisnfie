BuildSubsetQueue <- function(config) {
  
  buildFileNames <- function(subset) {
    switch(subset,
           national = {
             prefix <- paste0(config$netcdf$national, "/", suffix)
             dir.create(prefix, showWarnings = FALSE)
             paste0(prefix, "/", "national", "_", suffix, ".nc")
           },
           nfie_hydro = { 
             .BuildUniqueNames(config = config, 
                               subsetName = "nfie_hydro_region_num",
                               subsetMembers = .GetSubsetMembers(config, subsetName = "nfie_hydro_region_num", conn = conn),
                               prefix = config$netcdf$nfie_hydro,
                               suffix = suffix)
           },
           huc_l4 = {
             .BuildUniqueNames(config = config, 
                               subsetName = "huc_l4",
                               subsetMembers = .GetSubsetMembers(config, subsetName = "huc_cd", conn = conn),
                               prefix = config$netcdf$huc_l4, 
                               suffix = suffix)
           }, { 
             .BuildUniqueNames(config = config, 
                               subsetName = subset,
                               subsetMembers = .GetSubsetMembers(config, subsetName = subset, conn = conn), 
                               prefix = config$netcdf[[which(names(config$netcdf) == subset)]],
                               suffix = suffix)
           })
  }
  
  fileNames <- lapply(names(config$netcdf), buildFileNames)
  fileNames <- sapply(fileNames, c, recursive = TRUE)
  
  buildQueries <- function(subset) {
    switch(subset,
           national = {
             .GetNationalQuery(config = config, conn = conn)
           },
           nfie_hydro = { 
             .GetSubsetQueries(config = config, 
                               subsetName = "nfie_hydro_region_num",
                               subsetMembers = .GetSubsetMembers(config, subsetName = "nfie_hydro_region_num", conn = conn))
           },
           huc_l4 = {
             .GetSubsetQueries(config = config, 
                               subsetName = "huc_l4",
                               subsetMembers = .GetSubsetMembers(config, subsetName = "huc_cd", conn = conn))
           }, { 
             .GetSubsetQueries(config = config,
                               subsetName = subset,
                               subsetMembers = .GetSubsetMembers(config, subsetName = subset, conn = conn))
           })
  }
  
  queries <- lapply(names(config$netcdf), buildFileNames)
  queries <- sapply(queries, c, recursive = TRUE)
  
  data.frame(name = unlist(fileNames, recursive = FALSE), 
             query = unlist(queries, recursive = FALSE),
             stringsAsFactors = FALSE)
  
}



.BuildUniqueNames <- function(config, subsetName, subsetMembers, prefix, suffix) {
  
  NameBuilder <- function(member) {
    
    prefix <- paste0(prefix, "/", suffix)
    
    dir.create(prefix, showWarnings = FALSE)
    
    paste0(prefix,
           "/",
           subsetName,
           "_",
           member,
           "_",
           suffix,
           ".nc")
  }
  
  lapply(subsetMembers, NameBuilder)
  
}

.GetSubsetMembers <- function(config, subsetName, conn) {
  
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

.GetNationalQuery <- function(config, conn) {
  query <- paste0("select familyid from ", 
                  config$tables$site.metadata,
                  ";")
  
  list(query)
}

.GetSubsetQueries <- function(config, subsetMembers, subsetName) {
  
  buildQuery <- function(member) {
    query <- paste0("select familyid from ", 
                    config$tables$site.metadata,
                    " where ",
                    subsetName,
                    " = '",
                    member,
                    "'; ")
  }
  
  lapply(subsetMembers, buildQuery)
}

