#' Manage dynamic tables. 
#' 
#' The following tables are considered 'dynamic':
#' \enumerate{
#' \item data
#' \item staging
#' }
#' 
#' @name DynamicTables
#' @param config A configuration object created by LoadConfiguration
#' @return NULL
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @examples
#' \dontrun{
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  BuildDynamicTables(config)
#'  DropDynamicTables(config)
#'  # Equivalently...
#'  RebuildDynamicTables(config)
#'  }
NULL

#' @rdname DynamicTables
BuildDynamicTables <- function(config) {
  conn <- StartDBConnection(config)
  
  .CreateDynamicTables(conn = conn, config = config)
  .SetDynamicTriggers(conn = conn, config = config)
  .BuildDynamicIndices(conn = conn, config = config)
  
  StopDBConnection(conn = conn, config = config)
}

#' @rdname DynamicTables
DropDynamicTables <- function(config) {
  
  .message("Dropping all dynamic tables...", config = config)
  
  conn <- StartDBConnection(config)
  
  exists <- .WhichTablesExist(conn, config, quietly = TRUE)
  
  BuildAndRunQuery <- function(table){
    if (exists[table]) {
      
      .message(paste("Table '", 
                     config$tables[table],
                     "' (",
                     table,
                     ") exists.", 
                     sep = ""), 
               config = config)
      
      result <- RunQuery(conn = conn, 
                         query = paste("drop table ", 
                                       config$tables[table],
                                       ";", 
                                       sep = ""),
                         config = config)
      
      return(TRUE)
    } else {
      .message(paste("Table '", 
                     config$tables[table], 
                     "' (",
                     table,
                     ") does not exist.", 
                     sep = ""), 
               config = config)
      
      return(FALSE)
    }
  }
  
  dynamicTables <- list("data")
  
  cc <- lapply(dynamicTables, BuildAndRunQuery)
  
  .message("No dynamic tables remain.", config = config)
  
  cc <- StopDBConnection(conn = conn, config = config)
  
}

#' @rdname DynamicTables
RebuildDynamicTables <- function(config) {
  BuildDynamicTables(config)
  DropDynamicTables(config)
}

.CreateDynamicTables <- function(conn, config) {
  .CreateDataTable(conn = conn, config = config)
  .CreateStagingTable(conn = conn, config = config)
}

.SetDynamicTriggers <- function(conn, config) {
  .SetDataTriggers(conn = conn, config = config)
}

.BuildDynamicIndices <- function(conn, config) {
  .BuildDataIndices(conn = conn, config = config)
}

.CreateDataTable <- function(conn, config) {
  .message(paste("Building table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
  
  query <- paste("CREATE TABLE IF NOT EXISTS", config$tables$data,"
(ts timestamp with time zone NOT NULL,
seriesId text NOT NULL, 
familyId text, 
value numeric, 
paramcd text, 
validated integer, 
imported timestamp with time zone, 
updated timestamp with time zone, 
PRIMARY KEY(ts, seriesId) );")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste("Successfully built table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
}

.CreateStagingTable <- function(conn, config) {
  .message(paste("Building table ", 
                 config$tables$staging, 
                 "(",
                 names(config$tables$staging),
                 ").",
                 sep = ""), 
           config = config)
  
  query <- paste("CREATE TABLE IF NOT EXISTS", config$tables$staging,"
                 (id serial primary key, url text);")

  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste("Successfully built table ", 
                 config$tables$staging, 
                 "(",
                 names(config$tables$staging),
                 ").",
                 sep = ""), 
           config = config)
}

.SetDataTriggers <- function(conn, config) {
  
  .message(paste("Setting triggers on table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
  
  query = paste("CREATE OR REPLACE FUNCTION ", config$tables$data,"_upsert() RETURNS TRIGGER AS $$
BEGIN
  IF (SELECT COUNT(ts) FROM ", config$tables$data, 
                " WHERE ts = NEW.ts AND seriesid = NEW.seriesid) = 1 THEN
    UPDATE ", config$tables$data, " SET 
      updated = NEW.updated,
      validated = NEW.validated,
      value = NEW.value
      WHERE ts = NEW.ts AND seriesid = NEW.seriesid;
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER ", config$tables$data, "_data_merge BEFORE INSERT ON ", config$tables$data, " FOR EACH ROW EXECUTE PROCEDURE ", config$tables$data, "_upsert();", sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste("Triggers set for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
}

.BuildDataIndices <- function(conn, config) {
  
  .message(paste("Building indices for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
  
  query = paste("CREATE INDEX ",
                config$tables$data,
                "_combined_index on ", 
                config$tables$data, 
                "(ts, paramcd, seriesid);",
                sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  query = paste("CREATE INDEX ",
                config$tables$data,
                "_cross_index on ", 
                config$tables$data, 
                "(ts, paramcd, familyid);",
                sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  query = paste("CREATE INDEX ",
                config$tables$data,
                "_ts_index on ", 
                config$tables$data, 
                "(ts);",
                sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  query = paste("CREATE INDEX ",
                config$tables$data,
                "_familyid_index on ", 
                config$tables$data, 
                "(familyid);",
                sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  query = paste("CREATE INDEX ",
                config$tables$data,
                "_paramcd_index on ", 
                config$tables$data, 
                "(paramcd);",
                sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  query = paste("CREATE INDEX ", 
                config$tables$data,
                "_validated_index on ", 
                config$tables$data, 
                "(validated);",
                sep = "")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste("Successfully built indices for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
  
}
