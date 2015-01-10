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
LoadConfiguration <- function(configFile = "global_config.yaml"){
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
AssignLogFileToConfig <- function(file, config){
  config$logging$file = file
  return(config)
}