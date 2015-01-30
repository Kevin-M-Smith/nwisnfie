#' Parses configuration file and returns a \code{config} object. 
#' 
#' 
#' @param configFile Character vector describing path to configuration file in YAML format. 
#' @return Configuration object used by many functions in the \code{nwisnifie} package.
#' @seealso For proper formatting of the configuration file, see \code{\link{Configuration File}}.
#' @examples
#' \dontrun{
#' # Load Configuration File and Start Database Connection
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  conn <- StartDBConnection(config)
#' }
LoadConfiguration <- function(configFile = "global_config.yaml") {
  config <- yaml::yaml.load_file(configFile)
}

#' Adds a log file to a \code{config} object. 
#' 
#' @param config Configuration object created by LoadConfiguration.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @examples
#' \dontrun{
#'  library(nwisnfie)
#'  config <- LoadConfiguration("~/nwisnfie/global_config.yaml")
#'  config <- AssignLogFileToConfig(file = "~/nwisnfie/logs/myLog.log", config = config)
#' }
AssignLogFileToConfig <- function(file, config) {
  config$logging$file = file
  return(config)
}

.GetAllSites <- function(conn, config){
  query <- paste("SELECT site_no from ", config$tables$active.sites, ";", sep = "")
  sites <- RunQuery(conn = conn, query = query, config = config)
}

#' Internal function that throws error if file exists.
#' 
#' @param file File name in working directory. 
#' @return NULL
.ThrowErrorIfFileExists <- function(file){
  if (file.exists(file)){
    stop("      Previous installation detected at this location. 
         
         Please rename files that you wish to save, 
         delete other previously installed files, and try again.
         
         Alternatively, if you have no files you wish to save, use overwrite = TRUE.")
  }
}


.DropTable <- function(tableName, conn, config){
  query <- paste0("drop table ", tableName)
  
  RunQuery(query = query,
           conn = conn,
           config = config)
  
}

Upgrade <- function(){
  
  detach("package:nwisnfie", unload=TRUE)
  remove.packages("nwisnfie")
  
  library(devtools)
  install_github("Kevin-M-Smith/nwisnfie", ref = "better-mousetrap")
  library(nwisnfie)
}


#  queue <- BuildFileNamesAndLayerQueriesForAllSubsets(suffix = suffix, config = config, conn = conn)


