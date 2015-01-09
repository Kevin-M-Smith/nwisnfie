
LoadConfiguration <- function(configFile = "~/Desktop/Demo/global_config.yaml"){
  config <- yaml::yaml.load_file(configFile)
}