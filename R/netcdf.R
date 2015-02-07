BuildNetCDF <- function(data, queue, cluster, suffix, config, conn) {
  
  ##############################
  #   PREPARE SHARED DATA
  ##############################
  
  data$ts <- .ISO8601ToEpochTime(data$ts)
  layers <- sort(unique(data$familyid))
  times <- sort(unique(data$ts))  
  params <- unique(data$paramcd)
  
  siteMetadata <- .GetSiteMetadata(conn = conn, config = config)
  sensorMetadata <- .GetSensorMetadata(conn = conn, config = config) 
  
  paddedDataTable <- .BuildPaddedDataTable(layers = layers, 
                                           times = times,
                                           config = config)
  
  ##############################
  #   CONFIGURE SUBSETS
  ##############################
  parallel::clusterExport(cluster, "queue", envir = environment())
  parallel::clusterExport(cluster, "conn", envir = environment())
  parallel::clusterExport(cluster, "layers", envir = environment())
  
  ##############################
  #   
  #    BUILD NCDF FILES
  #	  AND ADD TIME AND METADATA
  #	  (NO PARAMETER DATA YET)f
  #
  ###############################
  .message(paste0("Adding metadata to NetCDF File(s)..."), config = config)
  
  pb <- txtProgressBar(min = 0, max = nrow(queue), style = 3, width = 20)
  cc <- foreach(i = 1:nrow(queue)) %dopar% {
    setTxtProgressBar(pb, i)
    
    layersInSubset <- RunQuery(conn = conn2,
                               query = queue$query[i],
                               config = config)[,1]
    
    layersInSubset <- layersInSubset[layersInSubset %in% layers]
    
    if(length(layersInSubset) >= 1) {
      
      layersInSubset <- sort(layersInSubset)
      
      siteMetadataSubset   <- subset(siteMetadata,   subset = familyid %in% layersInSubset)
      sensorMetadataSubset <- subset(sensorMetadata, subset = familyid %in% layersInSubset)
      
      # order
      siteMetadataSubset <- plyr::arrange(siteMetadataSubset, familyid)
      sensorMetadataSubset <- plyr::arrange(sensorMetadataSubset, familyid)
      
      ncdf <- .PrepareNetCDF(layers = layersInSubset, 
                             times = times, 
                             params = params,
                             siteMetadata = siteMetadataSubset,
                             sensorMetadata = sensorMetadataSubset,
                             file = queue$name[i], 
                             config = config)
      
      .AddTimeVars(ncdf = ncdf,
                   times = times,
                   config = config)
      
      .AddSensorMetadataVars(ncdf = ncdf,
                             sensorMetadata = sensorMetadataSubset,
                             layers = layersInSubset,
                             params = params,
                             config = config)
      
      .AddSiteMetadataVars(ncdf = ncdf, 
                           siteMetadata = siteMetadataSubset, 
                           layers = layersInSubset, 
                           params = params, 
                           config = config)    
      
      ncdf4::ncatt_put(ncdf, 
                       varid   = "v00065_value",
                       attname = "name",
                       attval  = "Gage Height")
      
      ncdf4::ncatt_put(ncdf, 
                       varid   = "v00065_value",
                       attname = "units",
                       attval  = "cubic feet per second")
      
      ncdf4::ncatt_put(ncdf, 
                       varid   = "v00060_value",
                       attname = "name",
                       attval  = "Discharge")
      
      ncdf4::ncatt_put(ncdf, 
                       varid   = "v00060_value",
                       attname = "units",
                       attval  = "feet")
      
      ncdf4::nc_close(ncdf)
      
    }    
  }
  rm(cc)
  cat("\n")
  
  ######################################################
  # Memory-Saver
  ################  
  dataFile <- tempfile()
  save("data", file = dataFile)
  rm(data)
  ######################################################
  
  ##############################
  #   
  #	  ADD VALUE AND VALIDATED
  #   VAR FOR A PARTICULAR
  #   PARAMETER
  #
  ###############################
  
  BulkAddValueAndValidatedVar <- function(parameterCode) {
    
    load(dataFile)
    
    paddedParamFlat <- merge(x = paddedDataTable, 
                             y = subset(data, paramcd == parameterCode), 
                             all.x = TRUE, 
                             by = c("ts", "familyid"))
    
    rm(data)
    
    paddedParamCast <- reshape2::dcast(paddedParamFlat, 
                                       familyid ~ ts, 
                                       value.var = "value")
    
    ######################################################
    # Memory-Saver
    ################
    paddedParamFlatFile <- tempfile()
    save("paddedParamFlat", file = paddedParamFlatFile)
    rm(paddedParamFlat)
    ######################################################
    
    name = paste("v", parameterCode, "_value", sep = "")
    .message(paste0("Adding data for ", name, " to NetCDF File(s)..."), config = config)
    
    ######################################
    #   For Each Subset, Add Value Vars
    ######################################
    pb <- txtProgressBar(min = 0, max = nrow(queue), style = 3, width = 20)
    cc <- foreach(i = 1:nrow(queue)) %dopar% {
      setTxtProgressBar(pb, i)
      
      layersInSubset <- RunQuery(conn = conn2,
                                 query = queue$query[i],
                                 config = config)[,1]
      
      layersInSubset <- layersInSubset[layersInSubset %in% layers]
      
      if(length(layersInSubset) >= 1) {
        
        subsetPaddedParamCast <- subset(paddedParamCast,
                                        subset = familyid %in% layersInSubset)
        
        subsetPaddedParamCast <- plyr::arrange(subsetPaddedParamCast, familyid)
        
        subsetPaddedParamCast <- subsetPaddedParamCast[, -1]
        
        subsetPaddedParamCast <- data.matrix(subsetPaddedParamCast)
        
        ncdf <- ncdf4::nc_open(queue$name[i], write = TRUE)
        
        ncdf4::ncvar_put(nc = ncdf, 
                         varid = name, 
                         vals = subsetPaddedParamCast, 
                         verbose = FALSE)  
        
        rm(subsetPaddedParamCast)
        
        ncdf4::nc_close(ncdf)
      }
    }
    rm(cc)
    cat("\n")
    
    
    #############################################
    #   For Each Subset, Add Validated Vars
    #############################################
    
    load(paddedParamFlatFile)
    
    paddedParamCast <- reshape2::dcast(paddedParamFlat, 
                                       familyid ~ ts, 
                                       value.var = "validated")
    
    rm(paddedParamFlatFile)
    
    name = paste("v", parameterCode, "_validated", sep = "")
    .message(paste0("Adding data for ", name, " to NetCDF File(s)..."), config = config)
    
    pb <- txtProgressBar(min = 0, max = nrow(queue), style = 3, width = 20)
    cc <- foreach(i = 1:nrow(queue)) %dopar% {
      setTxtProgressBar(pb, i)
      
      layersInSubset <- RunQuery(conn = conn2,
                                 query = queue$query[i],
                                 config = config)[,1]
      
      layersInSubset <- layersInSubset[layersInSubset %in% layers]
      
      if(length(layersInSubset) >= 1) {
        
        subsetPaddedParamCast <- subset(paddedParamCast,
                                        subset = familyid %in% layersInSubset)
        
        subsetPaddedParamCast <- plyr::arrange(subsetPaddedParamCast, familyid)
        
        subsetPaddedParamCast <- subsetPaddedParamCast[, -1]
        
        subsetPaddedParamCast <- data.matrix(subsetPaddedParamCast)
        
        ncdf <- ncdf4::nc_open(queue$name[i], write = TRUE)
        
        ncdf4::ncvar_put(nc = ncdf, 
                         varid = name, 
                         vals = subsetPaddedParamCast, 
                         verbose = FALSE)  
        
        rm(subsetPaddedParamCast)
        
        ncdf4::nc_close(ncdf)
      }
    }
    rm(cc)
    cat("\n")
  }
  
  lapply(params, BulkAddValueAndValidatedVar)
  
}

.PrepareNetCDF <- function(layers, times, params, siteMetadata, sensorMetadata, file, config){
  
  layerDim <- .BuildLayerDim(layers = layers, config = config)
  
  timeDim <- .BuildTimeDim(times = times, config = config)
  
  timeVar <- .BuildTimeVar(timeDim = timeDim, config = config)
  
  
  siteMetadataDims <- .BuildSiteMetadataDims(siteMetadata = siteMetadata,
                                             config = config)
  
  siteMetadataVars <- .BuildSiteMetadataVars(siteMetadata = siteMetadata,
                                             siteMetadataDims = siteMetadataDims,
                                             layerDim = layerDim,
                                             config = config)
  
  sensorMetadataDims <- .BuildSensorMetadataDims(params = params,
                                                 sensorMetadata = sensorMetadata,
                                                 config = config)
  
  sensorMetadataVars <- .BuildSensorMetadataVars(params = params,
                                                 sensorMetadataDims = sensorMetadataDims,
                                                 layerDim = layerDim,
                                                 config = config)  
  
  valueVars <- .BuildValueVars(params = params,
                               layerDim = layerDim,
                               timeDim = timeDim,
                               config = config)
  
  validatedVars <- .BuildValidatedVars(params = params,
                                       layerDim = layerDim,
                                       timeDim = timeDim,
                                       config = config)  
  
  ncdf <- .InitializeNCDF(file = file,
                          vars = c(list(timeVar),
                                   siteMetadataVars,
                                   sensorMetadataVars,
                                   valueVars,
                                   validatedVars),
                          config = config)
  
  ncdf
}

.GetSiteMetadata <- function(conn, config) {
  query <- paste("select * from", config$tables$site.metadata)
  result <- RunQuery(conn = conn,
                     query = query,
                     config = config)
  # familyid         site_no dd_nu                                         station_nm site_tp_cd dec_lat_va dec_long_va
  # 1 AZ011:340553110562745:00011:00001 340553110562745     1 PLEASANT VALLEY RANGER STATION PRECIP NR YOUNG, AZ         AT   34.09810   -110.9415
  # 2 AZ011:340553110562745:00011:00002 340553110562745     2 PLEASANT VALLEY RANGER STATION PRECIP NR YOUNG, AZ         AT   34.09810   -110.9415
  # 3 AZ011:340639111162945:00011:00001 340639111162945     1                GIESELA PRECIP GAGE NEAR PAYSON, AZ         AT   34.11087   -111.2754
  # 4 AZ011:340639111162945:00011:00002 340639111162945     2                GIESELA PRECIP GAGE NEAR PAYSON, AZ         AT   34.11087   -111.2754
  # 5 AZ011:342946111342645:00011:00001 342946111342645     1                   CROOK TRAIL PRECIP NEAR PINE, AZ         AT   34.49614   -111.5746
  # 6 AZ011:342946111342645:00011:00002 342946111342645     2                   CROOK TRAIL PRECIP NEAR PINE, AZ         AT   34.49614   -111.5746
  # dec_coord_datum_cd alt_va alt_datum_cd huc_cd tz_cd agency_cd district_cd county_cd country_cd
  # 1              NAD83   5184       NGVD29          MST     AZ011          04       007         US
  # 2              NAD83   5184       NGVD29          MST     AZ011          04       007         US
  # 3              NAD83   2920       NGVD29          MST     AZ011          04       007         US
  # 4              NAD83   2920       NGVD29          MST     AZ011          04       007         US
  # 5              NAD83   6300       NGVD29          MST     AZ011          04       025         US
  # 6              NAD83   6300       NGVD29          MST     AZ011          04       025         US
}

.GetSensorMetadata <- function(conn, config) {
  query <- paste("select * from", config$tables$sensor.metadata)
  result <- RunQuery(conn = conn,
                     query = query,
                     config = config)
  # familyid parm_cd loc_web_ds
  # 1 USGS:01010000:00011:00008   00065           
  # 2 USGS:01010000:00011:00018   00020           
  # 3 USGS:01010000:00011:00030   00011           
  # 4 USGS:01010000:00011:00005   00010           
  # 5 USGS:01010000:00011:00026   00021           
  # 6 USGS:01010000:00011:00006   00060           
  
}

.BuildLayerDim <- function(layers, config) {
  
  if(is.null(layers) || length(layers) < 1){
    n = 1
  } else {
    n = length(layers)
  }
  
  name = "layer_dim"
  dim <- ncdf4::ncdim_def(name = name, 
                          units = "", 
                          vals = 1:n, 
                          create_dimvar = FALSE)
  
  .debug(paste("Layer dim", 
               name,
               "built succesfully."),
         config = config)
  
  dim
}

.BuildTimeDim <- function(times, config)  { 
  
  if(is.null(times) || length(times) < 1){
    n = 1
  } else {
    n = length(times)
  }
  
  name <- "ts_dim"
  
  dim <- ncdf4::ncdim_def(name = name, 
                          units = "", 
                          vals = 1:n, 
                          create_dimvar = FALSE)
  
  .debug(paste("Time dimension", 
               name, 
               "built succesfully."), 
         config = config)
  dim
}

.BuildTimeVar <- function(timeDim, config) {
  name <- "time"
  var <- ncdf4::ncvar_def(name = name, 
                          units = "", 
                          dim = list(timeDim), 
                          prec = 'integer',
                          compression = 9)
  
  .debug(paste("Time variable ", 
               name, 
               " built succesfully.", sep = ""),
         config = config)
  var
}

.BuildSiteMetadataDims <- function(siteMetadata, config) {
  .debug("Building site metadata dimensions... ", config = config)
  
  varTypes <- lapply(siteMetadata, typeof)
  
  BuildSiteMetadataDim <- function(name){
    
    maxChar <- max(unlist(lapply(siteMetadata[name], nchar)))
    
    if(is.null(maxChar) || maxChar < 1) {
      n = 1
    } else {
      n = maxChar
    }
    
    name = paste(name, "Char", sep = "")
    
    dim <- ncdf4::ncdim_def(name = name, 
                            units = "", 
                            vals = 1:n, 
                            create_dimvar = FALSE)
    
    .debug(paste("Site metadata dimension ",
                 name,
                 " built succesfully.",
                 sep = ""),
           config = config)
    
    dim
  }
  
  # only string vars need extra dimension
  stringVars <- names(siteMetadata)[which(varTypes == "character")]
  lapply(stringVars, BuildSiteMetadataDim)
}

.BuildSiteMetadataVars <- function(siteMetadata, siteMetadataDims, layerDim, config) {
  
  .debug("Building site metadata variables... ", config = config)
  
  varTypes <- lapply(siteMetadata, typeof)
  stringVars <- names(siteMetadata)[which(varTypes == "character")]
  
  BuildSiteMetadataVar <- function(metadataVar) {
    
    if(varTypes[metadataVar] == 'character'){
      var <- ncdf4::ncvar_def(metadataVar, 
                              units = "", 
                              dim = list(siteMetadataDims[[which(stringVars == metadataVar)]], layerDim), 
                              prec = 'char',
                              compression = 9)    
    } else {
      if(varTypes[metadataVar] == 'integer'){
        var <- ncdf4::ncvar_def(metadataVar, 
                                units = "", 
                                dim = list(layerDim), 
                                prec = 'integer',
                                compression = 9)          
      } else {
        if(varTypes[metadataVar] == 'double'){
          var <- ncdf4::ncvar_def(metadataVar, 
                                  units = "", 
                                  dim = list(layerDim),
                                  missval = NA,
                                  prec = 'double',
                                  compression = 9)
        } else {
          .stop(paste("Could not find appropriate metadataVar type.", 
                      "(", 
                      varTypes[metadataVar], 
                      "),", 
                      metadataVar), 
                config = config)
        }
      }
    }
    .debug(paste("Site metadata variable ", 
                 metadataVar,
                 " built succesfully.",
                 sep = ""),
           config = config)
    
    var
  }
  
  lapply(names(siteMetadata), BuildSiteMetadataVar)
}

.BuildValueVars <- function(params, layerDim, timeDim, config) {
  
  .debug("Building value variables... ", config = config)
  
  BuildValueVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_value", sep = "")
    
    var <- ncdf4::ncvar_def(name = name,
                            units = "", 
                            dim = list(layerDim, timeDim), 
                            prec = "double",
                            compression = 9)
    
    .debug(paste("Value variable ",
                 name,
                 " built succesfully.",
                 sep = ""),
           config = config)
    
    var
  }
  
  lapply(params, BuildValueVar)
}

.BuildValidatedVars <- function(params, layerDim, timeDim, config) {
  
  .debug("Building validated variables... ", config = config)
  
  BuildValidatedVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_validated", sep = "")
    
    var <- ncdf4::ncvar_def(name = name,
                            units = "", 
                            dim = list(layerDim, timeDim), 
                            prec = "double",
                            compression = 9)
    
    .debug(paste("Validated variable ",
                 name,
                 " built succesfully.",
                 sep = ""),
           config = config)
    
    var
  }
  
  lapply(params, BuildValidatedVar)
}

.BuildSensorMetadataDims <- function(params, sensorMetadata, config) {
  
  .debug("Building sensor metadata dimensions... ", config = config)
  
  BuildSensorMetadataDim <- function(paramcd) {
    
    maxChar <- max(nchar(subset(sensorMetadata, parm_cd == paramcd)$loc_web_ds), 1)
    
    if(is.null(maxChar) || maxChar < 1) {
      n = 1
    } else {
      n = maxChar
    }
    
    name = paste("v", paramcd, "_descriptionChar", sep = "")
    
    dim <- ncdf4::ncdim_def(name = name, 
                            units = "", 
                            vals = 1:n, 
                            create_dimvar = FALSE)
    
    .debug(paste("Sensor metadata dimension ",
                 name,
                 " built succesfully",
                 sep = ""),
           config = config)
    
    dim
  }
  
  lapply(params, BuildSensorMetadataDim)
}

.BuildSensorMetadataVars <- function(params, sensorMetadataDims, layerDim, config) {
  
  .debug("Building sensor metadata variables... ", config = config)
  
  BuildSensorMetadataVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_description", sep = "")
    
    for (i in 1:length(sensorMetadataDims)) {
      match <- grep(name, sensorMetadataDims[[i]]$name)
      if (length(match) > 0) {
        match = i
        break
      }
    }
    
    var <- ncdf4::ncvar_def(name = name,
                            units = "", 
                            dim = list(sensorMetadataDims[[match]], layerDim), 
                            prec = 'char',
                            compression = 9)
    
    .debug("Sensor metadata variable ", 
           name, 
           " built successfully.",
           sep = "",
           config = config)
    var
  }
  
  lapply(params, BuildSensorMetadataVar)
}

.InitializeNCDF <- function(file, vars, config) {
  .debug(paste("Initializing empty NetCDF file (",
               file, 
               ").",
               sep = ""),
         config = config)
  
  # Create parent directory if it does not exist.
  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  
  ncdf <- ncdf4::nc_create(file = file, vars = vars, force_v4 = TRUE)
  
  .debug(paste("Successfully initialized empty NetCDF file (",
               file, 
               ").",
               sep = ""),
         config = config)
  
  ncdf
}

.BuildPaddedDataTable <- function(times, layers, config) {
  .debug(paste("Building padded data table. Total R memory usage: ", 
               capture.output(pryr::mem_used()),
               ".",
               sep = ""), 
         config = config)
  
  padded <- data.table::CJ(ts = times, familyid = layers)
  
  .debug(paste("Successfully built padded data table. Total R memory usage: ",
               capture.output(pryr::mem_used()),
               ".",
               sep = ""),
         config = config)
  
  padded
  
}

.AddTimeVars <- function(ncdf, times, config) {
  .debug(paste("Adding variable time to NetCDF file. Total R memory usage: ", 
               capture.output(pryr::mem_used()),
               ".",
               sep = ""), 
         config = config)
  
  
  ncdf4::ncvar_put(ncdf, "time", times)
  
  .debug(paste("Variable time added succesfully. Total R memory usage: ", 
               capture.output(pryr::mem_used()),
               ".",
               sep = ""), 
         config = config)
}


.AddSensorMetadataVars <- function(ncdf, sensorMetadata, layers, params, config) {
  .debug(paste("Adding sensor metadata variables to NetCDF file... Total R memory usage: ", 
               capture.output(pryr::mem_used()),
               ".",
               sep = ""), 
         config = config)
  
  AddSensorMetadataVar <- function(parameterCode) {
    
    name = paste("v", parameterCode, "_description", sep = "")
    
    .debug(paste("Adding sensor metadata to ",
                 name,
                 ". Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
    
    sub <- subset(sensorMetadata, parm_cd == parameterCode)
    sub <- plyr::join(data.frame(familyid = layers, stringsAsFactors = FALSE), sub, by = "familyid")
    
    ncdf4::ncvar_put(nc = ncdf,
                     varid = name,
                     vals = sub$loc_web_ds)
    
    .debug(paste("Successfully added sensor metadata to ",
                 name,
                 ". Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
    
  }
  
  lapply(params, AddSensorMetadataVar)
  
}

.AddSensorMetadataVars <- function(ncdf, sensorMetadata, layers, params, config) {
  .debug(paste("Adding sensor metadata variables to NetCDF file... Total R memory usage: ", 
               capture.output(pryr::mem_used()),
               ".",
               sep = ""), 
         config = config)
  
  AddSensorMetadataVar <- function(parameterCode) {
    
    name = paste("v", parameterCode, "_description", sep = "")
    
    .debug(paste("Adding sensor metadata to ",
                 name,
                 ". Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
    
    sub <- subset(sensorMetadata, parm_cd == parameterCode)
    sub <- plyr::join(x = data.frame(familyid = layers, stringsAsFactors = FALSE), 
                      y = sub, 
                      by = "familyid")
    
    ncdf4::ncvar_put(nc = ncdf,
                     varid = name,
                     vals = sub$loc_web_ds)
    
    .debug(paste("Successfully added sensor metadata to ",
                 name,
                 ". Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
    
  }
  
  lapply(params, AddSensorMetadataVar)
  
}

.AddSiteMetadataVars <- function(ncdf, siteMetadata, layers, params, config) {
  
  .debug(paste("Adding site metadata variables to NetCDF file... Total R memory usage: ", 
               capture.output(pryr::mem_used()),
               ".",
               sep = ""), 
         config = config)
  
  siteMetadata <- plyr::join(data.frame(familyid = layers, stringsAsFactors = FALSE), 
                             siteMetadata, 
                             by = "familyid")
  
  AddSiteMetadataVar <- function(name) {
    
    .debug(paste("Adding site metadata variable ",
                 name,
                 " to NetCDF file. Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
    
    ncdf4::ncvar_put(nc = ncdf,
                     varid = name,
                     vals = unlist(siteMetadata[name]))
    
    .debug(paste("Successfully added site metadata variable ",
                 name,
                 " to NetCDF file. Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
    
  }
  
  # excluded <- names(siteMetadata) %in% c("familyid")
  # siteMetadataVarNames <- names(siteMetadata)[!excluded]
  
  lapply(names(siteMetadata), AddSiteMetadataVar)
}

.CloseNetCDF <- function(ncdf, file, config){
  ncdf4::nc_close(ncdf)
  .debug(paste("Succesfully closed out NetCDF file (",
               file,
               ").",
               sep = ""),
         config = config)
}



