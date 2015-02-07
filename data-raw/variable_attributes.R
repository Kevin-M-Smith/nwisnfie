# This script is meant to be run from the 'data-raw' directory. 
# It imports variable attributes into 'R/sysdata.rda'
# Run this script whenever changes are made variable_attributes.csv.
# This script was originally run using: devtools_1.6.1  

library(devtools)

variable_attributes <- read.csv("variable_attributes.csv", stringsAsFactors=FALSE)
setwd("../")
devtools::use_data(variable_attributes, internal = TRUE, overwrite = TRUE)
