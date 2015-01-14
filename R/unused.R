test <- function(){
  # base dimensions
  layerDim <- ncdf4::ncdim_def("layer_dim", units = "", vals = 1:nLayers, create_dimvar = FALSE)
  timeDim <- ncdf4::ncdim_def("time_dim", units = "", vals = 1:nTimes, create_dimvar = FALSE)
  
  # time var
  timeVar <- ncdf4::ncvar_def("time", units = "", dim = dims, prec = 'integer')
  
  # v#####_value          double  :: layer, time
  buildParameterVars <- function(paramcode){
    ncvar_def(paste("v", paramcode, "_value", sep = ""),
              units = "", dim = list(layer_dim, ts_dim), prec = "double")
  }
  parameterVars <- lapply(params, buildParameterVars)
  
  # v#####_valiated       boolean :: layer, time
  buildValidatedVars <- function(paramcode){
    ncvar_def(paste("v", paramcode, "_validated", sep = ""), 
              units = "", dim = list(layer_dim, ts_dim), prec = 'double')
  }
  validatedVars <- lapply(params, buildValidatedVars)
  
  # v####_description
  dChar <- max(unlist(lapply(sensorMetadata, nchar)))
  dDim <- ncdim_def("descriptChar", units = "", vals = 1:dChar, create_dimvar = FALSE)
  
  buildSensorMetadataVars <- function(paramcode){
    ncvar_def(paste("v", paramcode, "_description", sep = ""), 
              units = "", dim = list(dDim, layer_dim), prec = 'char')
  }
  
  sensorMetadataVars <- lapply(params, buildSensorMetadataVars)
  
  varTypes <- lapply(meta.pad[1,], typeof)
  stringVars <- names(meta.pad)[which(varTypes == "character")]
  
  buildMetadataDims <- function(name){
    maxChar <- max(unlist(lapply(meta.pad[name], nchar)))
    ncdim_def(paste(name, "Char", sep = ""), units = "", vals = 1:maxChar, create_dimvar = FALSE )
  }
  
  metadataDims <- lapply(stringVars, buildMetadataDims)
  
  
  metadataVars <- lapply(names(metadata), buildMetadataVars)
  
}

NCBuild <- function(data, config){
  BuildNetCDF(data = data, 
              name = "test", 
              config = config, 
              conn = NULL)
}


Sample <- function(config) {
  
  conn <- StartDBConnection(config)
  
  query = "select * from data where ts > '2015-01-09' AND ts <= '2015-01-11';"
  
  data <- RunQuery(conn = conn, 
                   config = config,
                   query = query)
  
  StopDBConnection(conn = conn, config = config)
  
  return(data)
}

#  varTypes <- lapply(meta.pad[1,], typeof)


.AddSiteMetadata <- function(paramcd) {
  
}

.AddParameter <- function(paramcd, data, padding, dims, file, config) {
  
  name <- paste(paste("v", paramcd, sep = ""))
  
  data <- merge(x = padding, 
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
                   varid = paste(name, "_value", sep = ""),
                   vals = data[ , which(names(data) %in% "value")])
  
  ncdf4::ncvar_put(nc = nc,
                   varid = paste(name, "_validated", sep = ""),,
                   vals = data[ , -which(names(data) %in% "validated")])
  
  
  ncdf4::nc_close(nc)
}

.AddTime <- function(data, dims, file, config) {
  # time var
  nc <- ncdf4::nc_create(filename = file,
                         vars = timeVar,
                         verbose = TRUE)
  ncdf4::ncvar_put(nc = nc, 
                   varid = "time", 
                   vals = data)
  ncdf4::nc_close(nc)
} 

# lapply(params, AddParameterWrapper)
# 
# nc <- ncdf4::nc_open(file)
# print(nc)
# ncdf4::nc_close(nc)
# 
# sensorMetadata <- .GetSensorMetadata()
# siteMetadata <- .GetSiteMetadata()
# 
# if (logoutOnCompletion){
#   StopDBConnection(conn = conn, config = config)
# }


