#' Manage Database Connections
#' 
#' Functions used to start and stop database connections. 
#' 
#' @name DBConnections
#' @param conn A RPostgreSQL connection object. 
#' @param config Configuration object created by LoadConfiguration.
#' @return For \code{StartDBConnection}, a RPostgreSQL connection object.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @details
#' \code{StartDBConnection} starts a database connection specified by \code{config}.
#' \code{StopDBConnection} closes out the database connection contained in the \code{conn} object.
#' \code{TestDBConnection} attempts to start and stop a database connection based on \code{config}.
#' @examples
#' \dontrun{
#' # Start and End Database Connection
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  conn <- StartDBConnection(config)
#'  StopDBConnection(conn)
#' # or equivalently:
#'  TestDBConnection(config)
#' }
NULL

#' @rdname DBConnections
StartDBConnection <- function(config){
  library(RPostgreSQL)
  
  driver <- DBI::dbDriver("PostgreSQL")
  conn <- DBI::dbConnect(driver, 
                         dbname    = config$db$name, 
                         user      = config$db$user, 
                         host      = config$db$host,
                         password  = config$db$pass)
  
  .message("Database login successful.", config = config)
  return(conn)
}

#' @rdname DBConnections
StopDBConnection <- function(conn, config){
  cc <- DBI::dbDisconnect(conn)
  .message("Database logout successful.", config = config)
}

#' @rdname DBConnections
TestDBConnection <- function(config){
  conn <- StartDBConnection(config)
  StopDBConnection(conn = conn, config = config)
}

#' Runs specified query.
#' @param conn A RPostgreSQL connection object.
#' @param query A charactor vector of a PostgreSQL query. 
#' @return Result of query. 
#' @seealso To build a \code{conn} object, see \code{\link{StartDBConnection}}.
#' @examples
#' \dontrun{
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  conn <- StartDBConnection(config)
#'  result <- RunQuery(conn, "select * from data limit 1;")
#'  print(result)
#'  EndDBConnection(conn)
#' }
RunQuery <- function(conn, query, quietly = FALSE, config){
  if (missing(query)){
    .stop("No query specified for .RunQuery.", config = config)
  }
  
  res <- RPostgreSQL::dbGetQuery(conn, query)
  
  if (!is.null(res) & !quietly){
    .message("Successfully ran query: ", config = config)
    .message(query, config = config)
  }
}

#' Checks which tables exist. 
#' 
#' Checks for all tables listed in \code{\link{Configuration File}}.
#' 
#' @param conn Database connection created by StartDBConnection.
#' @param config A configuraiton object created by LoadConfiguration
#' @param quietly Logical, should the check proceed quietly?
#' @return NULL
#' @seealso To build a \code{conn} object, see \code{\link{StartDBConnection}}.
.WhichTablesExist <- function(conn, config, quietly = FALSE){
  
  BuildAndRunQuery <- function(table){
    result <- RunQuery(conn = conn, 
                       query = paste("select count(relname) from pg_class where relname = '", 
                                     table,
                                     "';", 
                                     sep = ""),
                       config = config)
    
    return(as.logical(result))
  }
  
  PrintHelper <- function(entry){
    .message(paste(entry[1], entry[2], entry[3], sep = "\t"), config = config)
  }
  
  cc <- sapply(config$tables, BuildAndRunQuery, USE.NAMES = TRUE)
  
  if (!quietly){
    if( sum(cc) > 0){
      .message("At least one existing table was found.", config = config)
      .message("Which tables exist:", config = config)
      apply(cbind(cc, config$tables, paste("(", names(config$tables), ")")), 1, PrintHelper)
    } else {
      .message("No tables found.", config = config)
    }
  }
  
  return(cc)
}

#' Run Diagnotistics on Database
#' 
#' The following tests are currently run:
#' \enumerate{
#' \item Load Database Connection
#' \item Check if required tables exist.
#' }
#' 
#' @param config A configuration object created by LoadConfiguration
#' @return NULL
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
RunDBDiagnostics <- function(config){
  conn <- StartDBConnection(config)
  .WhichTablesExist(conn = conn, config = config)
  StopDBConnection(conn = conn, config = config)
}

#' Manage Autovacuum Settings
#' 
#' Functions used to enable and disable autovacuum on the data table
#' 
#' @name Autovacuum
#' @param config Configuration object created by LoadConfiguration.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @details
#' \code{EnableAutovacuum} enables autovacuum on the data table specified in \code{config}.
#' \code{DisableAutovacuum} disables autovacuum on the data table specified in \code{config}.
NULL

#' @rdname Autovacuum
EnableAutoVacuum <- function(conn, config){
  conn <- StartDBConnection(config)
  
  .message(paste("Enabling autovacuum for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
  
  query <- paste("ALTER TABLE",
                config$tables$data,
                "SET (autovacuum_enabled = true);")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste("Succesfully enabled autovacuum for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
}

#' @rdname Autovacuum
DisableAutovacuum <- function(config){
  conn <- StartDBConnection(config)
  
  .message(paste("Disabling autovacuum for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
  
  query <- paste("ALTER TABLE",
                 config$tables$data,
                 "SET (autovacuum_enabled = false);")
  
  cc <- RunQuery(conn = conn, 
                 query = query, 
                 config = config)
  
  .message(paste("Succesfully disabled autovacuum for table ", 
                 config$tables$data, 
                 "(",
                 names(config$tables$data),
                 ").",
                 sep = ""), 
           config = config)
}