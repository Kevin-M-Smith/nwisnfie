This page lists the files and respective functions in the `nwisnfie` package under the `R` directory. 

<hr>

## [database.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/database.R)
#### Exported: 
* RunDBDiagnostics(config)
* RunQuery(conn, query, quietly = FALSE, config)
* TestDBConnection(config)
* StopDBConnection(conn, config)
* StartDBConnection(config)
* EnableAutoVacuum(table, config)
* DisableAutoVacuum(table, config)

#### Non-Exported:
* .WhichTablesExist(conn, config, quietly = FALSE)

## [downloaders.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/downloaders.R)
#### Non-Exported: 
* .DownloadAssetsForSites(sites, config)
* .DownloadActiveSitesForState(state, config)
* .DownloadDataFromNWIS(sites, params, startDate, endDate, offset = NULL, tableName, config)  

## [dynamic_tables.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/dynamic_tables.R)
#### Non-Exported:
.CreateDataTable(conn, config, tableName)
.CreateDataTableUpsertTrigger(conn, config, tableName)
.DropDataTableUpsertTrigger(conn, config, tableName)


## [helpers.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/helpers.R)
#### Exported:
* LoadConfiguration(configFile = "global_config.yaml")
* Upgrade() __@TODO: Unimplemented__

#### Non-Exported:
* .GetAllSites <- function(conn, config)              		__@TODO: Undocumented__
* .ThrowErrorIfFileExists(file)
* .DropTable(tableName, conn, config)

## [install.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/install.R)
#### Exported:
* InstallFiles(installDirectory, overwrite = FALSE)

## [datetime.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/datetime.R)
* .ISO8601ToEpochTime(ISO8601)
* .ParseISO8601Duration(duration)
* .ParseISO8601Offset(offset)

## [netcdf.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/netcdf.R)
#### Exported:
* BuildNetCDF(data, name, config, conn = NULL)                  __@TODO: Undocumented__

#### Non-Exported:
* .PrepareNetCDF(layers, times, params, siteMetadata, sensorMetadata, file, config)
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

## [parallel.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/parallel.R)
#### Exported: 
* StartCluster(config)
* StopCluster(cluster, config)
* TestClusterSettings(config)
* StartClusterDBConnection(cluster, config)     __@TODO: Undocumented__
* StopClusterDBConnections(cluster, config)     __@TODO: Undocumented__

## [logging.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/logging.R)
#### Exported:
* AssignLogFileToConfig(file, config)
              
#### Non-Exported:                                         
* .PrintLogMessage(..., config, domain = NULL, level) 
* .debug(..., config, domain = NULL, appendLF = TRUE)
* .message(..., config, domain = NULL, appendLF = TRUE)
* .stop(..., config, call. = TRUE, domain = NULL)
* .warning(..., config, call. = TRUE, immediate. = FALSE, domain = NULL)

## [static_tables.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/static_tables.R)
#### Exported:
* PopulateStaticTables(config)
* DropStaticTables(config)
* WhichStaticTablesExist(config)
* RepopulateStaticTables(config)

#### Non-Exported:
* .PopulateActiveSites(conn, config)             
* .PopulateSiteAssetsfunction(conn, config)
* .PopulateParamCodes(conn, config)
* .PopulateParamMetadata(conn, config)          __@TODO: Not Implemented__
* .PopulateSensorMetadata(conn, config)
* .PopulateSiteMetadata(conn, config)

##[subsetting.R](https://github.com/Kevin-M-Smith/nwisnfie/tree/master/R/subsetting.R)
#### Exported:
* BuildFileNamesAndLayerQueriesForAllSubsets __@TODO: Undocumented__
#### Non-Exported:
* .BuildUniqueNames
* .GetUniqueSubsets
* .GetNationalSites
* .GetSubsetSites