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
#' \dontrun{
#' # Start and End Database Connection
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  conn <- StartDBConnection(config)
#'  StopDBConnection(conn)
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
  
  cat("Database Login Successful. \n")
  return(conn)
}

#' @rdname DBConnections
StopDBConnection <- function(conn){
  cc <- DBI::dbDisconnect(conn)
  cat("Database Logout Successful. \n")
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
RunQuery <- function(conn, query){
  if (missing(query)){
    stop("No query specified for .RunQuery.")
  }
  
  res <- RPostgreSQL::dbGetQuery(conn, query)
}

#' Checks which tables exist. 
#' 
#' Checks for all tables listed in \code{\link{Configuration File}}.
#' 
#' @param conn Database connection created by StartDBConnection.
#' @param config A configuraiton object created by LoadConfiguration
#' @return NULL
#' @seealso To build a \code{conn} object, see \code{\link{StartDBConnection}}.
WhichTablesExist <- function(conn, config){
  
  BuildAndRunQuery <- function(table){
    result <- RunQuery(conn = conn, 
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
  WhichTablesExist(conn = conn, config = config)
}

