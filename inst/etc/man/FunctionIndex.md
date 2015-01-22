This page lists the files and respective functions in the `nwisnfie` package under the `R` directory. 

<hr>

## all_tables.R
* DropAllTables(config)       					__@TODO: Undocumented__

## bootstrap.R	
* Bootstrap(config)						__@TODO: Undocumented__

## database.R
#### Exported: 
* RunDBDiagnostics(config)
* RunQuery(conn, query, config)
* TestDBConnection(config)
* StopDBConnection(conn, config)
* StartDBConnection(config)
* EnableAutoVacuum(conn, config)
* DisableAutoVacuum(conn, config)

#### Non-Exported
* .WhichTablesExist(conn, config, quietly = FALSE)

## debug.R
* DownloadOneSite(config)              			__@TODO: Undocumented__
* Sample24H(config)					__@TODO: Undocumented__

## downloaders.R
* .DownloadAssetsForSites(sites, config)
* .DownloadActiveSitesForState(state, config)
* .DownloadDataFromNWIS(sites, params, startDate = NULL, endDate = NULL, period = NULL, offset = NULL, url = NULL, config)  
* .RetryDownloadDataFromNWIS(config, url)			

## dynamic_tables.R
* DropDynamicTables(config)           
* BuildDynamicTables(config)
* RebuildDynamicTables(config)
* .CreateDynamicTables(conn, config)
* .SetDynamicTriggers(conn, config)
* .BuildDynamicIndices(conn, config)
* .CreateStagingTable(conn, config)
* .SetDataTriggers(conn, config)
* .BuildDataIndices(conn, config)

## helpers.R
#### Exported Functions
* LoadConfiguration(configFile = "global_config.yaml")
* AssignLogFileToConfig(file, config)

#### Non-Exported Functions
* .GetAllSites <- function(conn, config)              		__@TODO: Undocumented__
* .ThrowErrorIfFileExists(file)

## install.R
#### Exported Functions
* InstallFiles(installDirectory, overwrite = FALSE)

## netcdf.R
#### Exported Functions
* BuildNetCDF(data, name, config, conn = NULL)                  __@TODO: Undocumented__

#### Non-Exported Functions
* .ISO8601ToEpochTime(ISO8601)
* .GetSiteMetadata(conn, config)
* .GetSensorMetadata(conn, config)
* .BuildLayerDim(layers, config)
* .BuildTimeDim(times, config)
* .BuildTimeVar(timeDim, config)
* .BuildSiteMetadataDims(siteMetadata, config)
* .BuildSiteMetadataVars(siteMetadata, siteMetadataDims, layerDim, config)
* .BuildValueVars(params, layerDim, timeDim, config)
* .BuildValidatedVars(params, layerDim, timeDim, config)
* .BuildSensorMetadataDims(params, sensorMetadata, config)
* .BuildSensorMetadataVars(params, sensorMetadataDims, layerDim, config)
* .InitializeNCDF(file, vars, config)
* .BuildPaddedDataTable(times, layers, config)
* .AddTimeVars(ncdf, times, config)
* .AddValueAndValidatedVars(ncdf, padded, data, params, config)
* .AddSensorMetadataVars(ncdf, sensorMetadata, layers, params, config)
* .AddSensorMetadataVars(ncdf, sensorMetadata, layers, params, config)
* .AddSiteMetadataVars(ncdf, siteMetadata, layers, params, config)
* .CloseNetCDF(ncdf, file, config)

## parallel.R
* StartCluster(config)
* StopCluster(cluster, config)
* TestClusterSettings(config)

## logging.R                                                      
* .PrintLogMessage(..., config, domain = NULL, level) 
* .message(..., config, domain = NULL, appendLF = TRUE)
* .stop(..., config, call. = TRUE, domain = NULL)
* .warning(..., config, call. = TRUE, immediate. = FALSE, domain = NULL)

## static_tables.R
#### Exported Functions
* PopulateStaticTables(config)
* DropStaticTables(config)
* WhichStaticTablesExist(config)
* RepopulateStaticTables(config)

#### Non-Exported Functions
* .PopulateActiveSites(conn, config)             
* .PopulateSiteAssetsfunction(conn, config)
* .PopulateParamCodes(conn, config)
* .PopulateParamMetadata(conn, config)          __@TODO: Not Implemented__
* .PopulateSensorMetadata(conn, config)
* .PopulateSiteMetadata(conn, config)

## sync.R
#### Exported Functions
* SyncDB(config, period)			 __@TODO Undocumented__
