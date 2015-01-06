# This script is meant to be run from the 'data-raw' directory. 
# It imports the parameter codes into 'R/sysdata.rda'
# Run this script whenever changes are made to parameter_codes.rdb.
# Note that the expected format of parameter_codes.rdb is the USGS RDBv1 tab delimted format.
# This script was originally run using: dataRetrieval_2.1.0 and devtools_1.6.1  

library(dataRetrieval)
library(devtools)

parameter_codes <- dataRetrieval::importRDB1("parameter_codes.rdb")
setwd("../")
devtools::use_data(parameter_codes, internal = TRUE)
