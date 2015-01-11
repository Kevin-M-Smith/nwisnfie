DropDynamicTables <- function(config) {
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