library(nwisnfie)

config <- LoadConfiguration()

PopulateStaticTables(config)


.CreateDataTable <- function(conn, config, tableName) {
  
  .message(paste0("Building table ", tableName, "."), config = config)
  
  query <- paste("CREATE TABLE IF NOT EXISTS", tableName,"
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
  
  .message(paste0("Successfully built table ", tableName, "."), config = config)
  
}
(config)

Bootstrap(config)