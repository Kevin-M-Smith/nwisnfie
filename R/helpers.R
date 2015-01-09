#' Parses configuration file and returns a \code{config} object. 
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