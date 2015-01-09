#' Runs specified query.
#' @param configFile Path to configuration file in YAML format. 
#' @param conn Database connection created by DBI::dbDriver
.RunQuery <- function(conn, query){
  if (missing(query)){
    stop("No query specified for .RunQuery.")
  }
  
  res <- RPostgreSQL::dbGetQuery(conn, query)
}

#' Checks which tables exist. 
#' @param configFile Path to configuration file in YAML format. 
#' @param conn Database connection created by DBI::dbDriver
.WhichTablesExist <- function(conn, config){
  
  BuildAndRunQuery <- function(table){
    result <- .RunQuery(conn = conn, 
              query = paste("select count(relname) from pg_class where relname = '", 
                    table,
                    "';", 
                    sep = "")
              )
   
    cat(paste("\t", table, "\t\t\t\t", as.logical(result), "\n", sep = ""))
    return(as.logical(result))
  }
  
  cat("Which Tables Exist: \n")
  
  if( sum(sapply(config$tables, BuildAndRunQuery)) > 0){
    cat("At least one existing table was found.")
  } else {
    cat("No tables found.")
  }
  
}


StartDBConnection <- function(config){
  library(RPostgreSQL)

  driver <- DBI::dbDriver("PostgreSQL")
  
  conn <- DBI::dbConnect(driver, 
                         dbname    = config$db$name, 
                         user      = config$db$user, 
                         host      = config$db$host,
                         password  = config$db$pass)
  
  cat("Database Login Successful. \n")
  
  return(conn)
}

EndDBConnection <- function(conn){
  cc <- DBI::dbDisconnect(conn)
  cat("Database Logout Successful. \n")
}

RunDiagnostics <- function(config){
  conn <- StartDBConnection(config)
  .WhichTablesExist(conn = conn, config = config)
}

PopulateStaticTables <- function(config){
  conn <- StartDBConnection(config)
  
  .StartupCluster(config)
  
  .PopulateParamCodes(conn = conn, config = config)
  .PopulateActiveSites(conn = conn, config = config)
}

.PopulateParamCodes <-function(conn, config){
  RPostgreSQL::dbWriteTable(conn = conn, 
                            name = config$tables$param.codes, 
                            parameter_codes)
}

.PopulateActiveSites <- function(conn, config){
  pb <- txtProgressBar(min = 1, max = 49, style = 3, width = 20)
  
  cc <- foreach(i = 1) %dopar% { 
    DownloadActiveSitesForState(state.abb[i]) 
  }
  
  cc <- foreach(i = 2:50) %dopar% {
    setTxtProgressBar(pb, i)
    DownloadActiveSitesForState(state.abb[i]) 
  }
  
}
