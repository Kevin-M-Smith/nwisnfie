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


PrepareNetCDF <- function(layers, times, params, siteMetadata, sensorMetadata, file, config){
  
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