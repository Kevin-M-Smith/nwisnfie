.ISO8601ToEpochTime <- function(ISO8601) {
  time1 <- as.POSIXct(ISO8601)
  time0 <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
  seconds <- as.numeric(difftime(time1, time0, units="secs"))
  gettextf("%.0f", seconds)
}

.ParseISO8601Duration <- function(period) {
  
  period <- toupper(period)
  
  struct <- "P(([0-9]*)Y)?(([0-9]*)M)?(([0-9]*)W)?(([0-9]*)D)?(T(([0-9]*)H)?(([0-9]*)M)?(([0-9]*)S)?)?"
  
  parsed <- stringr::str_match(period, struct)
  
  lubridate::new_period(
    year   = ifelse(parsed[, 3] == "", 0, as.numeric(parsed[,3])),
    month  = ifelse(parsed[, 5] == "", 0, as.numeric(parsed[,5])),
    week   = ifelse(parsed[, 7] == "", 0, as.numeric(parsed[,7])),
    day    = ifelse(parsed[, 9] == "", 0, as.numeric(parsed[,9])),
    hour   = ifelse(parsed[,12] == "", 0, as.numeric(parsed[,12])),
    minute = ifelse(parsed[,14] == "", 0, as.numeric(parsed[,14])),
    second = ifelse(parsed[,16] == "", 0, as.numeric(parsed[,16])))
  
}

.ParseISO8601Offset <- function(offset) {
  
  struct <- "(-)?([0-2][0-9]):([0-5][0-9])" 
  parsed <- stringr::str_match(offset, struct)
  
  signedUnity <- ifelse(parsed[,2] == "-", -1, 1)
  hour <- ifelse(parsed[,3] == "", 0, as.numeric(parsed[,3])) * signedUnity
  minute <- ifelse(parsed[,4] == "", 0, as.numeric(parsed[,4])) * signedUnity
  
  lubridate::new_period(
    hour = hour,
    minute = minute)
  
}



