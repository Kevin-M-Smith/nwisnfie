BuildAllNetCDFSubsets2 <- function(data, cluster, suffix, config, conn) {

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
  queue <- BuildFileNamesAndLayerQueriesForAllSubsets(suffix = suffix, config = config, conn = conn)
  #parallel::clusterExport(cluster, "queue", envir = environment())
  #parallel::clusterExport(cluster, "conn", envir = environment())
  
  ##############################
  #   
  #	  BUILD NCDF FILES
  #	  AND ADD TIME AND METADATA
  #	  (NO PARAMETER DATA YET)
  #
  ###############################
  cc <- foreach(i = 1:5) %dopar% {
  	
  	layersInSubset <- RunQuery(conn = conn2,
                          query = queue$query[i],
                          config = config)[,1]
	
	siteMetadataSubset   <- subset(siteMetadata,   subset = familyid %in% layersInSubset)
  sensorMetadataSubset <- subset(sensorMetadata, subset = familyid %in% layersInSubset)
	
	ncdf <- PrepareNetCDF(layers = layersInSubset, 
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
    
    ncdf4::nc_close(ncdf)
    
	}
	
  ##############################
  #   
  #	  ADD VALUE AND VALIDATED
  #   VAR FOR A PARTICULAR
  #   PARAMETER
  #
  ###############################
	
  BulkAddValueAndValidatedVar <- function(paramcd){
  
  paddedParamFlat <- merge(x = paddedDataTable, 
    			 	y = subset(data, paramcd == paramcd), 
    			 	all.x = TRUE, 
    			 	by = c("ts", "familyid"))
  
  paddedParamCast <- reshape2::dcast(paddedParamFlat, 
  									familyid ~ ts, 
  									value.var = "value")
  									
  name = paste("v", paramcd, "_value", sep = "")

   cc <- foreach(i = 1:5) %dopar% {
        
        ncdf <- ncdf4::nc_open(queue$name[i], write = TRUE)
		
		layersInSubset <- RunQuery(conn = conn2,
                          query = queue$query[i],
                          config = config)[,1]
                          
        subsetPaddedParamCast <- subset(paddedParamCast,
        								subset = familyid %in% layersInSubset)[, -1]
        								
        subsetPaddedParamCast <- data.matrix(subsetPaddedParamCast)
        
        .debug(paste0("Dim: ", dim(subsetPaddedParamCast)), config = config)
        
        ncdf4::ncvar_put(nc = ncdf, 
        				varid = name, 
        				vals = subsetPaddedParamCast, 
        				verbose = TRUE)  
        
        ncdf4::nc_close(ncdf)
   }

   lapply(params, BulkAddValueAndValidatedVar)

  }
  
}