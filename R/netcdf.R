
library(nwisnfie)


tc <- function(X){
  time1 <- as.POSIXct(X)
  # time1 <- fast_strptime(X, format="%Y-%m-%dT%H:%M:%OS%z")
  time0 <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
  sec <- as.numeric(difftime(time1, time0, units="secs"))
  gettextf("%.0f", sec)
}


config <- LoadConfiguration("~/Desktop/Demo2/global_config.yaml")
conn <- StartDBConnection(config)

query = "select * from data2 where ts > '2015-01-09' AND ts <= '2015-01-11';"

data <- RunQuery(conn = conn, 
                   config = config,
                   query = query)


library(data.table)
library(pryr)

#res <- CJ(unique(result["paramcd"]), unique(result["familyid"]), unique(result["ts"]))

# nLayers
layers <- sort(unique(data$familyid))
nLayers <- length(layers)

# nTimes
data$ts <- tc(data$ts)
times <- sort(unique(data$ts))
nTimes <- length(times)

# nParams
params <- unique(data$paramcd)
nParams <- length(params)

# build padded table
data.pad1 <- expand.grid(ts = times, familyid = layers, paramcd = params, stringsAsFactors = FALSE)

data.pad2 <- CJ(ts = times, familyid = layers)





