configurationTest <- function() {
  
  if (is.null(ncores)){
    if (config$parallel$max.cores == "NONE"){
      ncores = parallel::detectCores()
    } else if (is.numeric(config$parallel$max.cores)){
      if (config$parallel$max.cores > parallel::detectCores()){
        ncores = parallel::detectCores()
      } else {
        ncores = config$parallel$max.cores
      }
    } else {
      warning("
            Did not recognize configuration for parallel/max.cores. 
            Defaulting to single core operations. 
            
            Please specify maximum number of cores numerically (e.g. 4) 
              or NONE to use all available cores.

            EXAMPLES
            parallel:
             max.cores: NONE

            parallel:
             max.cores: 2")
    }
  }
  
  ########################################
  #
  #     Have this change the 
  #     in-memory configuration.
  #
  ########################################
}