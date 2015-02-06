CustomMetadata <- function(fileName, config){
  
  fileName = "~/Downloads/national_2014-05-05.nc"

  ncdf <- ncdf4::nc_open(fileName)

  ncdf4::ncatt_put(ncdf, 
                   varid   = 0,
                   attname = "featureType",
                   attval  = "timeSeries")

  ncdf4::ncatt_put(ncdf,
                   varid   = 0,
                   attname = "cdm_data_type",
                   attval  = "station")

}