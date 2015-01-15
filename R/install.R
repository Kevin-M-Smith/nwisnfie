# This file contains functions related to installation and configuration. 

# Documents the Configuration File
#' Configuration File
#' 
#' This package uses a configuration file to manage basic operational details. It is written in YAML syntax.
#' 
#' @references For the most up-to-date standard file template, see \url{https://github.com/Kevin-M-Smith/nwisnfie/blob/master/inst/etc/nwisnfie/global_config.yaml}.
#' @references For information on the YAML specification, see \url{http://www.yaml.org/spec/1.2/spec.html}.
#' @name Configuration File
NULL

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


#' Installs scripts and configuration files to the specified directory. 
#' 
#' If the directory does not exist, one will be created. 
#' An error will be thrown if previous installation exists in the directory, unless overwrite == TRUE. 
#' 
#' @param installDirectory Directory to to 
#' @param overwrite Overwrite previously installed files?
#' @return NULL
InstallFiles <- function(installDirectory, overwrite = FALSE){
  if (missing(installDirectory)) {
    stop(simpleError("You must specify an installation directory."))
  }
  
  internalPath <- system.file("etc", "nwisnfie", package = "nwisnfie")
  
  if (!file.exists(installDirectory)){
    cat("Installation directory not found. \n");
    tryCatch({
      dir.create(installDirectory)
    }, warning = function(w){
      print(w)
      stop("There was a problem creating the directory.")
    }, error = function(e){
      print(e)
      stop("There was a problem creating the directory.")
    })
    cat("Installation directory built successfully. \n")
    
  } else {
    if(overwrite == FALSE){
      installationFileNames <- list.files(internalPath, 
                                          recursive = TRUE, 
                                          include.dirs = TRUE, 
                                          full.names = FALSE)
      setwd(installDirectory)
      sapply(installationFileNames, .ThrowErrorIfFileExists)
    }
  }
  
  tryCatch({
    file.copy(from = list.files(internalPath, 
                                recursive = TRUE, 
                                include.dirs = TRUE, 
                                full.names = TRUE),
              to = installDirectory,
              recursive = TRUE)
  }, warning = function(w){
    print(w)
    stop("There was a problem installing files.")
  }, error = function(e){
    print(e)
    stop("There was a problem installing files.")
  })
  
  cat("Files installed successfully: \n")
  cat(installDirectory)
  cat("\n")
}


