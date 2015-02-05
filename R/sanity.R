SanityCheckNetCDF <- function(fileName, config) {
  
  cluster <- StartCluster(config, ncores = config$parallel$max.cores)
  
  fileName = "~/Downloads/national_2014-05-05.nc"
  ncdf <- ncdf4::nc_open(fileName)
  
  times <- ncdf4::ncvar_get(ncdf, "time")
  times <- as.POSIXct(times, origin = "1970-01-01")
  
  familyids <- ncdf4::ncvar_get(ncdf, "familyid")
  siteNumbers <- ncdf4::ncvar_get(ncdf, "site_no")
  methodIDs <- ncdf4::ncvar_get(ncdf, "dd_nu")
  
  ##########################################
  # 
  ##########################################
  
  .SanityCheckValueVars(ncdf = ncdf, 
                        times = times, 
                        siteNumbers = siteNumbers,
                        methodIDs = methodIDs,
                        cluster = cluster,
                        config = config)
  
  .SanityCheckValidatedVars(ncdf = ncdf,
                            times = times,
                            siteNumbers = siteNumbers,
                            methodIDs = methodIDs,
                            cluster = cluster,
                            config = config)
  
  .SanityCheckMetadata(fileName = fileName,
                       times = times,
                       siteNumbers = siteNumbers,
                       methodIDs = methodIDs,
                       cluster = cluster,
                       config = config)
  
  StopCluster(cluster = cluster, config = config)
  
}


.SanityCheckMetadata  <- function(fileName, times, siteNumbers, methodIDs, cluster, config) {
  
  ncdf <- ncdf4::nc_open(fileName)
    
  times.format <- strftime(times, format = "%Y-%m-%dT%H:%M:%S%z")
  
  extractParams <- function(index) {
    line <- capture.output(print(ncdf))[index]
    stringr::str_match(line, ".*v([0-9]{5})_value.*")[2]
  }
  
  params <- sapply(grep("value", capture.output(print(ncdf))), extractParams)
  
  names <- paste0("v", params,"_value")
  params <- cbind(params, names)
  
  checkParam <- function(row) {
    
    vals <- ncdf4::ncvar_get(ncdf, row[2])
    vals[vals == -999999.00] <- NA
    
    withData <- which(rowSums(is.na(vals)) != ncol(vals))
    
    siteNumbers <- siteNumbers[withData]
    vals <- vals[withData, ]
    methodIDs <- methodIDs[withData]
    
    numberToCheck <- min(length(withData), 10)
    
    layersToCheck <- sample(1:length(siteNumbers), 
                            size = numberToCheck, 
                            replace = FALSE)
    
    .message(paste0("Checking ",
                    row[2],
                    " metadata against live NWIS data for ",
                    numberToCheck,
                    " sites."), 
             config = config)
    
    parallel::clusterExport(cluster, "times", envir = environment())
    parallel::clusterExport(cluster, "times.format", envir = environment())
    parallel::clusterExport(cluster, "methodIDs", envir = environment())
    parallel::clusterExport(cluster, "fileName", envir = environment())
    
    pb <- txtProgressBar(min = 0, max = numberToCheck, style = 3, width = 20)
    cc <- foreach(i = 1:numberToCheck) %dopar% {
      setTxtProgressBar(pb, i)
      ncdf <- ncdf4::nc_open(fileName)
          
      layerSelect <- layersToCheck[i]
      
      siteNumber <- siteNumbers[layerSelect]
      valsSubset <- vals[layerSelect, ]
      methodID   <- methodIDs[layerSelect]
      
      data <- data.frame(times = times, ncdf.value = valsSubset)
      data <- data[complete.cases(data),]
      
      url <- dataRetrieval::constructNWISURL(siteNumber = siteNumber, 
                                             parameterCd = row[1], 
                                             startDate = min(times.format), 
                                             endDate = max(times.format), 
                                             service = "uv")
      
      url <- paste0(url, "&methodId=", methodID)
      
      xml <- RCurl::basicTextGatherer()
      
      responseCode <- RCurl::curlPerform(url = url, 
                                         writefunction = xml$update, 
                                         httpheader = c(AcceptEncoding="gzip,deflate")) 
      
      doc <- XML::xmlTreeParse(xml$value(), getDTD = FALSE, useInternalNodes = TRUE) 
      
      doc <- XML::xmlRoot(doc)
      
      xpath <- "//ns1:timeSeries"
      
      doc <- XML::xpathApply(doc = doc, path = xpath)[[1]]
      doc <- XML::xmlDoc(doc) 
      doc <- XML::xmlRoot(doc) 
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteName", fun = XML::xmlValue)) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "station_nm")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- c("station_nm", "PASS")
      } else {
        results <- c("station_nm", "FAIL")
      }
      .debug(paste0("station_nm", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("station_nm", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteCode", fun = XML::xmlValue)) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "site_no")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("site_no", "PASS"))
      } else {
        results <- rbind(results, c("site_no", "FAIL"))
      }
      .debug(paste0("site_no", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("site_no", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteProperty[@name=\"siteTypeCd\"]", fun = XML::xmlValue)) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "site_tp_cd")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("site_tp_cd", "PASS"))
      } else {
        results <- rbind(results, c("site_tp_cd", "FAIL"))
      }
      .debug(paste0("site_tp_cd", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("site_tp_cd", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteProperty[@name=\"hucCd\"]", fun = XML::xmlValue)) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "huc_cd")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("huc_cd", "PASS"))
      } else {
        results <- rbind(results, c("huc_cd", "FAIL"))
      }
      .debug(paste0("huc_cd", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("huc_cd", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteProperty[@name=\"stateCd\"]", fun = XML::xmlValue)) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "district_cd")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("district_cd", "PASS"))
      } else {
        results <- rbind(results, c("district_cd", "FAIL"))
      }
      .debug(paste0("district_cd", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("district_cd", " : NCDF : ", testVarNCDF), config = config)
      
      # "47071" = StateCode: ("47") County Code: ("071")
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteProperty[@name=\"countyCd\"]", fun = XML::xmlValue)) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "county_cd")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("county_cd", "PASS"))
      } else {
        results <- rbind(results, c("county_cd", "FAIL"))
      }  
      .debug(paste0("county_cd", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("county_cd", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:siteCode/@agencyCode")) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "agency_cd")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("agency_cd", "PASS"))
      } else {
        results <- rbind(results, c("agency_cd", "FAIL"))
      }  
      .debug(paste0("agency_cd", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("agency_cd", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:method/@methodID")) 
      testVarNCDF <- ncdf4::ncvar_get(ncdf, "dd_nu")[withData]
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("dd_nu", "PASS"))
      } else {
        results <- rbind(results, c("dd_nu", "FAIL"))
      } 
      .debug(paste0("dd_nu", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("dd_nu", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:latitude", fun = XML::xmlValue)) 
      testVarNCDF <- as.character(ncdf4::ncvar_get(ncdf, "dec_lat_va")[withData])
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("dec_lat_va", "PASS"))
      } else {
        results <- rbind(results, c("dec_lat_va", "FAIL"))
      }  
      .debug(paste0("dec_lat_va", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("dec_lat_va", " : NCDF : ", testVarNCDF), config = config)
      
      testVarWXML <- unlist(XML::xpathApply(doc = doc, path = "//ns1:longitude", fun = XML::xmlValue)) 
      testVarNCDF <- as.character(ncdf4::ncvar_get(ncdf, "dec_long_va")[withData])
      testVarNCDF <- testVarNCDF[layerSelect]
      if(testVarWXML == testVarNCDF){
        results <- rbind(results, c("dec_long_va", "PASS"))
      } else {
        results <- rbind(results, c("dec_long_va", "FAIL"))
      }  
      .debug(paste0("dec_long_va", " : WXML : ", testVarWXML), config = config)
      .debug(paste0("dec_long_va", " : NCDF : ", testVarNCDF), config = config)
      
      #.message(capture.output(cat(results)), config = config)
      
      warnAboutFailures <- function(variable) {
        if(variable[2] == "FAIL"){
          .message(paste0(variable[1], " failed check."), config = config)
        }
      }
      
      apply(results, 1, warnAboutFailures)
    }  
  }
  apply(params, 1, checkParam) 
}

.SanityCheckValueVars <- function(ncdf, times, siteNumbers, methodIDs, cluster, config) {
  
  times.format <- strftime(times, format = "%Y-%m-%dT%H:%M:%S%z")
  
  extractParams <- function(index) {
    line <- capture.output(print(ncdf))[index]
    stringr::str_match(line, ".*v([0-9]{5})_value.*")[2]
  }
  
  params <- sapply(grep("value", capture.output(print(ncdf))), extractParams)
  
  names <- paste0("v", params,"_value")
  params <- cbind(params, names)
  
  checkParam <- function(row) {
    
    vals <- ncdf4::ncvar_get(ncdf, row[2])
    vals[vals == -999999.00] <- NA
    
    withData <- which(rowSums(is.na(vals)) != ncol(vals))
    
    siteNumbers <- siteNumbers[withData]
    vals <- vals[withData, ]
    methodIDs <- methodIDs[withData]
    
    numberToCheck <- min(length(withData), 10)
    
    layersToCheck <- sample(1:length(siteNumbers), 
                            size = numberToCheck, 
                            replace = FALSE)
    
    .message(paste0("Checking ",
                    row[2],
                    " against live NWIS data for ",
                    numberToCheck,
                    " sites."), 
             config = config)
    
    parallel::clusterExport(cluster, "times", envir = environment())
    parallel::clusterExport(cluster, "times.format", envir = environment())
    parallel::clusterExport(cluster, "methodIDs", envir = environment())
    
    pb <- txtProgressBar(min = 0, max = numberToCheck, style = 3, width = 20)
    cc <- foreach(i = 1:numberToCheck) %dopar% {
      setTxtProgressBar(pb, i)
      
      layerSelect <- layersToCheck[i]
      
      siteNumber <- siteNumbers[layerSelect]
      valsSubset <- vals[layerSelect, ]
      methodID   <- methodIDs[layerSelect]
      
      data <- data.frame(times = times, ncdf.value = valsSubset)
      data <- data[complete.cases(data),]
      
      url <- dataRetrieval::constructNWISURL(siteNumber = siteNumber, 
                                             parameterCd = row[1], 
                                             startDate = min(times.format), 
                                             endDate = max(times.format), 
                                             service = "uv")
      
      url <- paste0(url, "&methodId=", methodID)
      
      nwis <- dataRetrieval::importWaterML1(url, asDateTime = TRUE, tz = "")
      
      if(nrow(nwis) == 0){
        print(paste0(row[1], " is empty."))
      } else {
        nwis <- nwis[,c(3,6)]
        colnames(nwis) <- c("times", "nwis.value")
        data <- plyr::join(x = data, y = nwis, by = "times")
        
        matching <- sum(data$nwis.value == data$ncdf.value) / nrow(data) * 100;
        
        if(matching != 100){
          print(paste0(matching, "% of the data matched."))
        }
        
      }
    }  
  }
  apply(params, 1, checkParam) 
}

.SanityCheckValidatedVars <- function(ncdf, times, siteNumbers, methodIDs, cluster, config) {
  
  times.format <- strftime(times, format = "%Y-%m-%dT%H:%M:%S%z")
  
  extractParams <- function(index) {
    line <- capture.output(print(ncdf))[index]
    stringr::str_match(line, ".*v([0-9]{5})_value.*")[2]
  }
  
  params <- sapply(grep("value", capture.output(print(ncdf))), extractParams)
  
  names <- paste0("v", params,"_validated")
  params <- cbind(params, names)
  
  checkParam <- function(row) {
    
    vals <- ncdf4::ncvar_get(ncdf, row[2])
    vals[vals == -999999.00] <- NA
    
    withData <- which(rowSums(is.na(vals)) != ncol(vals))
    
    siteNumbers <- siteNumbers[withData]
    vals <- vals[withData, ]
    methodIDs <- methodIDs[withData]
    
    numberToCheck <- min(length(withData), 10)
    
    layersToCheck <- sample(1:length(siteNumbers), 
                            size = numberToCheck, 
                            replace = FALSE)
    
    .message(paste0("Checking ",
                    row[2],
                    " against live NWIS data for ",
                    numberToCheck,
                    " sites."), 
             config = config)
    
    parallel::clusterExport(cluster, "times", envir = environment())
    parallel::clusterExport(cluster, "times.format", envir = environment())
    parallel::clusterExport(cluster, "methodIDs", envir = environment())
    
    pb <- txtProgressBar(min = 0, max = numberToCheck, style = 3, width = 20)
    cc <- foreach(i = 1:numberToCheck) %dopar% {
      setTxtProgressBar(pb, i)
      
      layerSelect <- layersToCheck[i]
      
      siteNumber <- siteNumbers[layerSelect]
      valsSubset <- vals[layerSelect, ]
      methodID   <- methodIDs[layerSelect]
      
      data <- data.frame(times = times, ncdf.value = valsSubset)
      data <- data[complete.cases(data),]
      
      url <- dataRetrieval::constructNWISURL(siteNumber = siteNumber, 
                                             parameterCd = row[1], 
                                             startDate = min(times.format), 
                                             endDate = max(times.format), 
                                             service = "uv")
      
      url <- paste0(url, "&methodId=", methodID)
      
      nwis <- dataRetrieval::importWaterML1(url, asDateTime = TRUE, tz = "")
      
      IsDataValidated <- function(x){
        if(x == "A") 1 else 0
      }
      
      nwis[,5] <- sapply(nwis[,5], IsDataValidated)
      
      if(nrow(nwis) == 0){
        print(paste0(row[1], " is empty."))
      } else {
        nwis <- nwis[,c(3,5)]
        colnames(nwis) <- c("times", "nwis.value")
        data <- plyr::join(x = data, y = nwis, by = "times")
        
        matching <- sum(data$nwis.value == data$ncdf.value) / nrow(data) * 100;
        
        if(matching != 100){
          print(paste0(matching, "% of the data matched."))
        }
        
      }
    }  
  }
  apply(params, 1, checkParam)
  
}




