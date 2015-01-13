.ISO8601ToEpochTime <- function(ISO8601) {
  time1 <- as.POSIXct(ISO8601)
  time0 <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
  seconds <- as.numeric(difftime(time1, time0, units="secs"))
  gettextf("%.0f", sec)
}

BuildNetCDF <- function(data, name, config, conn = NULL) {
  
  if (is.null(conn)){
    logoutOnCompletion = TRUE
    conn <- StartDBConnection(config)
  } else {
    logoutOnCompletion = FALSE
  }
  
  # output file
  file <- paste(tempdir(), name, sep = "/")
  
  # nLayers
  layers <- sort(unique(data$familyid))
  nLayers <- length(layers)
  
  # nTimes
  data$ts <- .ISO8601ToEpochTime(data$ts)
  times <- sort(unique(data$ts))
  nTimes <- length(times)
  
  # nParams
  params <- unique(data$paramcd)
  nParams <- length(params)
  
  # base dimensions
  layerDim <- ncdf4::ncdim_def("layer_dim", units = "", vals = 1:nLayers, create_dimvar = FALSE)
  timeDim <- ncdf4::ncdim_def("time_dim", units = "", vals = 1:nTimes, create_dimvar = FALSE)
  
  # padded data
  padding <- data.table::CJ(ts = times, familyid = layers, paramcd = params)
  
  # 0. Add Time & Initialize  
  .AddTime(data = times, dims = list(timeDim), file = file, config = config)
  
  # 1. Add Parameters
  AddParameterWrapper <- function(paramcd) {
    .AddParameter(paramcd = paramcd,
                  data = subset(x = data, paramcd == paramcd),
                  padding = padding,
                  dims = list(layerDim, timeDim),
                  file = file,
                  config = config)
  }
  
  lapply(params, AddParameterWrapper)
  
  sensorMetadata <- .GetSensorMetadata()
  siteMetadata <- .GetSiteMetadata()
  
  if (logoutOnCompletion){
    StopDBConnection(conn = conn, config = config)
  }
  
}


#  varTypes <- lapply(meta.pad[1,], typeof)


.AddSiteMetadata <- function(paramcd) {
  
}

.AddParameter <- function(paramcd, data, padding, dims, file, config) {
  
  name <- paste(paste("v", paramcd, sep = ""))
  
  data <- data.table::merge(x = padding, 
                            y = data, 
                            by = c("ts", "familyid"), 
                            all.x = TRUE, 
                            all.y = FALSE)
  
  paramVar <- ncdf4::ncvar_def(name = paste(name, "_value", sep = ""),
                               units = "",
                               dim = dims,
                               prec = "double")
  
  validVar <- ncdf4::ncvar_def(name = paste(name, "_validated", sep = ""),
                               units = "",
                               dim = dims,
                               prec = "double")
  
  nc <- ncdf4::nc_open(filename = file, write = TRUE)
  
  ncdf4::ncvar_put(nc = nc,
                   varid = name,
                   vals = data[ , -which(names(data) %in% "validated")])
  
  ncdf4::ncvar_put(nc = nc,
                   varid = name,
                   vals = data[ , -which(names(data) %in% "value")])
  
  ncdf4::nc_close(nc)
}

.AddTime <- function(data, dims, file, config) {
  # time var
  timeVar <- ncdf4::ncvar_def("time", units = "", dim = dims, prec = 'integer')
  nc <- ncdf4::nc_create(timeVar)
  ncdf4::ncvar_put(nc = nc, 
                   varid = "time", 
                   vals = times)
  ncdf4::nc_close(nc)
} 

.GetSiteMetadata <- function(conn, config) {
  query <- paste("select * from", config$tables$site.metadata)
  result <- RunQuery(conn = conn,
                     query = query,
                     config = config)
}

.GetSensorMetadata <- function(conn, config) {
  descriptionQuery <- paste("select * from", config$tables$sensor.metadata)
  result <- RunQuery(conn = conn,
                     query = query,
                     config = config)
}
