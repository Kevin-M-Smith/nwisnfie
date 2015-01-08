

CheckDatabaseCredentials <- function(configFile = "global_config.yaml"){
  config <- yaml::yaml.load_file(configFile)
  
  driver <- DBI::dbDriver("PostgreSQL")
  
  con <- DBI::dbConnect(driver, 
                        dbname    = config$db$name, 
                        user      = config$db$user, 
                        host      = config$db$host,
                        password  = config$db$pass)
  
  cat("Database Login Successful.")
}

