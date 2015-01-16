.ISO8601ToEpochTime <- function(ISO8601) {
  time1 <- as.POSIXct(ISO8601)
  time0 <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
  seconds <- as.numeric(difftime(time1, time0, units="secs"))
  gettextf("%.0f", seconds)
}


BuildNetCDF <- function(data, name, config, conn = NULL) {
  
  if (is.null(conn)){
    logoutOnCompletion = TRUE
    conn <- StartDBConnection(config)
  } else {
    logoutOnCompletion = FALSE
  }
  
  # output file
  #file <- paste(tempdir(), "name23", sep = "/")
  file <- "~/Desktop/test2.nc"
  
  siteMetadata <- .GetSiteMetadata(conn = conn, config = config)
  
  sensorMetadata <- .GetSensorMetadata(conn = conn, config = config)
  
  if (logoutOnCompletion == TRUE){
    StopDBConnection(conn = conn, config = config)
  } 
  
  layers <- sort(unique(data$familyid))
  layerDim <- .BuildLayerDim(layers = layers, config = config)
  
  data$ts <- .ISO8601ToEpochTime(data$ts)
  times <- sort(unique(data$ts))  
  timeDim <- .BuildTimeDim(times = times, config = config)
  timeVar <- .BuildTimeVar(timeDim = timeDim, config = config)
  
  params <- unique(data$paramcd)
  
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
  
  paddedDataTable <- .BuildPaddedDataTable(layers = layers, 
                                           times = times,
                                           config = config)
  
  .AddTimeVars(ncdf = ncdf,
               times = times,
               config = config)
  
  .AddValueAndValidatedVars(ncdf = ncdf,
                            padded = paddedDataTable,
                            data = data,
                            params = params,
                            config = config)
  
  .AddSensorMetadataVars(ncdf = ncdf,
                         sensorMetadata = sensorMetadata,
                         layers = layers,
                         params = params,
                         config = config)
  
  .AddSiteMetadataVars(ncdf = ncdf, 
                       siteMetadata = siteMetadata, 
                       layers = layers, 
                       params = params, 
                       config = config)
  
  .CloseNetCDF(ncdf = ncdf, file = file, config = config)
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
  name = "layer_dim"
  dim <- ncdf4::ncdim_def(name = name, 
                          units = "", 
                          vals = 1:length(layers), 
                          create_dimvar = FALSE)
  
  .message(paste("Layer dim", 
                 name,
                 "built succesfully."),
           config = config)
  
  dim
}

.BuildTimeDim <- function(times, config)  { 
  name <- "ts_dim"
  
  dim <- ncdf4::ncdim_def(name = name, 
                          units = "", 
                          vals = 1:length(times), 
                          create_dimvar = FALSE)
  
  .message(paste("Time dimension", 
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
  
  .message(paste("Time variable ", 
                 name, 
                 " built succesfully.", sep = ""),
           config = config)
  var
}

.BuildSiteMetadataDims <- function(siteMetadata, config) {
  .message("Building site metadata dimensions... ", config = config)
  
  varTypes <- lapply(siteMetadata, typeof)
  
  BuildSiteMetadataDim <- function(name){
    
    maxChar <- max(unlist(lapply(siteMetadata[name], nchar)))
    
    name = paste(name, "Char", sep = "")
    
    dim <- ncdf4::ncdim_def(name = name, 
                            units = "", 
                            vals = 1:maxChar, 
                            create_dimvar = FALSE)
    
    .message(paste("Site metadata dimension ",
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
  
  .message("Building site metadata variables... ", config = config)
  
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
    .message(paste("Site metadata variable ", 
                   metadataVar,
                   " built succesfully.",
                   sep = ""),
             config = config)
    
    var
  }
  
  lapply(names(siteMetadata), BuildSiteMetadataVar)
}

.BuildValueVars <- function(params, layerDim, timeDim, config) {
  
  .message("Building value variables... ", config = config)
  
  BuildValueVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_value", sep = "")
    
    var <- ncdf4::ncvar_def(name = name,
                            units = "", 
                            dim = list(layerDim, timeDim), 
                            prec = "double",
                            compression = 9)
    
    .message(paste("Value variable ",
                   name,
                   " built succesfully.",
                   sep = ""),
             config = config)
    
    var
  }
  
  lapply(params, BuildValueVar)
}

.BuildValidatedVars <- function(params, layerDim, timeDim, config) {
  
  .message("Building validated variables... ", config = config)
  
  BuildValidatedVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_validated", sep = "")
    
    var <- ncdf4::ncvar_def(name = name,
                            units = "", 
                            dim = list(layerDim, timeDim), 
                            prec = "double",
                            compression = 9)
    
    .message(paste("Validated variable ",
                   name,
                   " built succesfully.",
                   sep = ""),
             config = config)
    
    var
  }
  
  lapply(params, BuildValidatedVar)
}

.BuildSensorMetadataDims <- function(params, sensorMetadata, config) {
  
  .message("Building sensor metadata dimensions... ", config = config)
  
  BuildSensorMetadataDim <- function(paramcd) {
    
    maxChar <- max(nchar(subset(sensorMetadata, parm_cd == paramcd)$loc_web_ds))
    maxChar <- max(maxChar, 2)
    
    name = paste("v", paramcd, "_descriptionChar", sep = "")
    
    dim <- ncdf4::ncdim_def(name = name, 
                            units = "", 
                            vals = 1:maxChar, 
                            create_dimvar = FALSE)
    
    .message(paste("Sensor metadata dimension ",
                   name,
                   " built succesfully",
                   sep = ""),
             config = config)
    
    dim
  }
  
  lapply(params, BuildSensorMetadataDim)
}

.BuildSensorMetadataVars <- function(params, sensorMetadataDims, layerDim, config) {
  
  .message("Building sensor metadata variables... ", config = config)
  
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
    
    .message("Sensor metadata variable ", 
             name, 
             " built successfully.",
             sep = "",
             config = config)
    var
  }
  
  lapply(params, BuildSensorMetadataVar)
}


.InitializeNCDF <- function(file, vars, config) {
  .message(paste("Initializing empty NetCDF file (",
                 file, 
                 ").",
                 sep = ""),
           config = config)
  
  ncdf <- ncdf4::nc_create(file = file, vars = vars, force_v4 = TRUE)
  
  .message(paste("Successfully initialized empty NetCDF file (",
                 file, 
                 ").",
                 sep = ""),
           config = config)
  
  ncdf
}

.BuildPaddedDataTable <- function(times, layers, config) {
  .message(paste("Building padded data table. Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
  
  padded <- data.table::CJ(ts = times, familyid = layers)
  
  .message(paste("Successfully built padded data table. Total R memory usage: ",
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""),
           config = config)
  
  padded
  
}

.AddTimeVars <- function(ncdf, times, config) {
  .message(paste("Adding variable time to NetCDF file. Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
  
  
  ncdf4::ncvar_put(ncdf, "time", times)
  
  .message(paste("Variable time added succesfully. Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
}

.AddValueAndValidatedVars <- function(ncdf, padded, data, params, config) {
  
  
  .message(paste("Adding value and validated variables to NetCDF file... Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
  
  AddValueAndValidatedVar <- function(paramcd){
    
    .message(paste("Subsetting data for ", 
                   paramcd,
                   ". Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    sub <- subset(data, paramcd == paramcd)
    sub <- sub[c("ts", "familyid", "value", "validated")]
    
    .message(paste("Merging subsetted data for ",
                   paramcd,
                   ". Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    sub <- merge(x = padded, y = sub, all.y = TRUE, by = c("ts", "familyid"))
    
    name = paste("v", paramcd, "_value", sep = "")
    
    .message(paste("Properly casting data for ",
                   name,
                   ". Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    val <- data.matrix(reshape2::dcast(sub, familyid ~ ts, value.var = "value"))[, -1]
    
    .message(paste("Adding data for ",
                   name,
                   " into NetCDF File. Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    ncdf4::ncvar_put(nc = ncdf, varid = name, vals = val)
    
    .message(paste("Succesfully added ",
                   name,
                   " to NetCDF File. Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    name = paste("v", paramcd, "_validated", sep = "")
    
    .message(paste("Properly casting data for ",
                   name,
                   ". Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    val <- reshape2::dcast(sub, familyid ~ ts, value.var = "value")
    val <- data.matrix(val)[, -1]
    
    .message(paste("Adding data for ",
                   name,
                   " into NetCDF File. Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    ncdf4::ncvar_put(nc = ncdf, varid = name, vals = val)
    
    .message(paste("Succesfully added ",
                   name,
                   " to NetCDF File. Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
  }
  
  lapply(params, AddValueAndValidatedVar)
  
  .message(paste("Added value and validated variables to NetCDF file. Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
}

.AddSensorMetadataVars <- function(ncdf, sensorMetadata, layers, params, config) {
  .message(paste("Adding sensor metadata variables to NetCDF file... Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
  
  AddSensorMetadataVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_description", sep = "")
    
    .message(paste("Adding sensor metadata to ",
                   name,
                   ". Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    sub <- subset(sensorMetadata, parm_cd == paramcd)
    sub <- plyr::join(data.frame(familyid = layers, stringsAsFactors = FALSE), sub, by = "familyid")
    
    ncdf4::ncvar_put(nc = ncdf,
                     varid = name,
                     vals = sub$loc_web_ds)
    
    .message(paste("Successfully added sensor metadata to ",
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
  .message(paste("Adding sensor metadata variables to NetCDF file... Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
  
  AddSensorMetadataVar <- function(paramcd) {
    
    name = paste("v", paramcd, "_description", sep = "")
    
    .message(paste("Adding sensor metadata to ",
                   name,
                   ". Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
    sub <- subset(sensorMetadata, parm_cd == paramcd)
    sub <- plyr::join(data.frame(familyid = layers, stringsAsFactors = FALSE), sub, by = "familyid")
    
    ncdf4::ncvar_put(nc = ncdf,
                     varid = name,
                     vals = sub$loc_web_ds)
    
    .message(paste("Successfully added sensor metadata to ",
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
  
  .message(paste("Adding site metadata variables to NetCDF file... Total R memory usage: ", 
                 capture.output(pryr::mem_used()),
                 ".",
                 sep = ""), 
           config = config)
  
  siteMetadata <- plyr::join(data.frame(familyid = layers, stringsAsFactors = FALSE), 
                             siteMetadata, 
                             by = "familyid")
  
  AddSiteMetadataVar <- function(name) {
    
    .message(paste("Adding site metadata variable ",
                   name,
                   " to NetCDF file. Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
   ncdf4::ncvar_put(nc = ncdf,
                    varid = name,
                    vals = unlist(siteMetadata[name]))
    
    .message(paste("Successfully added site metadata variable ",
                   name,
                   " to NetCDF file. Total R memory usage: ", 
                   capture.output(pryr::mem_used()),
                   ".",
                   sep = ""), 
             config = config)
    
  }
  
#  excluded <- names(siteMetadata) %in% c("familyid")
 # siteMetadataVarNames <- names(siteMetadata)[!excluded]
  
  lapply(names(siteMetadata), AddSiteMetadataVar)
}

.CloseNetCDF <- function(ncdf, file, config){
  ncdf4::nc_close(ncdf)
  .message(paste("Succesfully closed out NetCDF file (",
           file,
           ").",
           sep = ""),
           config = config)
}


