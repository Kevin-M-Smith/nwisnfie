.DownloadDataFromNWISTest <- function(sites, 
                                  params, 
                                  startDate = NULL, 
                                  endDate = NULL, 
                                  period = NULL, 
                                  offset = NULL,
                                  url = NULL,
                                  tableName = config$tables$data,
                                  config){
  
  if (is.null(url)) {
    url = "http://waterservices.usgs.gov/nwis/iv/?format=waterml,1.1"
    url = paste(url, "&sites=", sites, sep = "")  
    url = paste(url, "&parameterCd=", params, sep = "")
    
    if (is.null(startDate) || is.null(endDate)){
      if (is.null(period)){
        .stop("A lookback period or a pair of start 
              and end dates must be specified.", 
              config = config)
      } else {
        url = paste(url, "&period=", period, sep = "")  
      }
    } else {
      if (is.null(period)){
        if (is.null(offset)){
          url = paste(url, "&startDT=", startDate, "T00:00:00", sep = "")
          url = paste(url, "&endDT=", endDate, "T23:59:59", sep = "")
        } else {
          url = paste(url, "&startDT=", startDate, "T00:00:00", offset, sep = "")
          url = paste(url, "&endDT=", endDate, "T23:59:59", offset, sep = "")
        }
      } else {
        .stop("Please choose either a lookback period or a pair of start 
              and end dates, but not both.", 
              config = config)
      }
    }
    }
  
  
  xml <- RCurl::basicTextGatherer()
  
  for(i in 1:5) {
    t <- tryCatch({
      
      responseCode <- RCurl::curlPerform(url = url, 
                                         writefunction = xml$update, 
                                         httpheader = c(AcceptEncoding="gzip,deflate")) 
      
      if(responseCode == 0){
        break
      } else {
        Sys.sleep(1)
      }
      
    }, COULDNT_RESOLVE_HOST = function(e) { 
      .warning(paste0("Couldn't resolve url: ", 
                      url,
                      "\n",
                      5-i,
                      " attempts remaining."), 
               config = config)
      
      Sys.sleep(1)
    },
    error = function(e) {
      .warning(paste0(e, 
                      "\n Trouble with url: ",
                      url,
                      "\n",
                      5-i,
                      " attempts remaining."),
               config = config)
      
      Sys.sleep(1)
    })
  }
  
  if(responseCode == 0){
    
  } else {
    .warning(paste0("Url failed permanently: ",
                    url,
                    "."),
             config = config)
    
    return(NULL)
  }
  
  t <- tryCatch({
    doc <- XML::xmlTreeParse(xml$value(), getDTD = FALSE, useInternalNodes = TRUE) 
  }, XMLError = function(e) {
    .warning(paste0("There was an error in the XML at line", 
                    e$line, 
                    "column", 
                    e$col, 
                    "\n",
                    e$message, 
                    "\n for URL :",
                    url, "."),
             config = config)
    
  }, warning = function(e) { 
    error(e) 
  }, error = function(e) {
    .warning(paste0(e, 
                    "\n Trouble with xml from ",
                    url,
                    "."),
             config = config)
    
    return(NULL)
  }) 
  
  doc <- XML::xmlRoot(doc)
  
  t <- tryCatch({
    xpath <- "//ns1:timeSeries"
    vars <- XML::xpathApply(doc = doc, path = xpath) 
  }, error = function(e) {
    .message(paste0("Error attempting xpathApply on", 
                    xpath), 
             config = config)
    return(NULL)
  })
  
  now <- format(Sys.time(), "%FT%T%z") 
  
  IsDataValidated <- function(x){
    if(x == "P") 0 else 1
  }
  
  if(length(vars) > 0){
    tryCatch({
      for (i in 1:length(vars)){ 
        parent <- XML::xmlDoc(vars[[i]]) 
        parent <- XML::xmlRoot(parent) 
        parentName <- unlist(XML::xpathApply(parent, "//ns1:timeSeries/@name")) 
        sensors <- XML::xpathApply(parent, "//ns1:values") 
        parameter <- XML::xpathApply(parent, "//ns1:variableCode", XML::xmlValue)
        familyName <- paste(unlist(strsplit(parentName, ":", fixed = TRUE))[-3], collapse = ":")
        for (j in 1:length(sensors)){ 
          child <- XML::xmlDoc(sensors[[j]]) 
          child <- XML::xmlRoot(child) 
          if(!is.null(unlist(XML::xpathApply(child, "//@dateTime")))){
            childName <- unlist(XML::xpathApply(child, "//ns1:method/@methodID")) 
            childName <- formatC(strtoi(childName), width = 5, format = "d", flag = "0")  
            
            result <- data.frame( 
              unlist(XML::xpathApply(child, "//@dateTime")), 
              paste(parentName, ":", childName, sep = ""), 
              paste(familyName, ":", childName, sep = ""),
              unlist(XML::xpathApply(child, "//ns1:value", XML::xmlValue)),
              parameter, 
              unlist(lapply(XML::xpathApply(child, "//@qualifiers"), IsDataValidated)), 
              now, 
              now 
            ) 
            
            colnames(result) <- c("ts", "seriesid", "familyid", "value", "paramcd", "validated", "imported", "updated") 
            
            print(result)
            
            cc <- RPostgreSQL::dbWriteTable(conn2, 
                                            name = tableName, 
                                            value = result, 
                                            append = TRUE, 
                                            row.names = FALSE, 
                                            overwrite = FALSE) 
          }
        }
      }    
    }, error = function(e){
      .warning(paste0(e, 
                      "\nEncountered a problem parsing XML from:", 
                      url,
                      "."),
               config = config)
      
      return(NULL)
    })
  } 
  }
