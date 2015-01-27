#' Download Active Sites for State
#' 
#' Downloads the 'Active Sites' in a given state.
#' 
#' @param state A two character code specifying a state. (e.g. "AK" for Alaska)
#' @param config Configuration object created by LoadConfiguration.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
#' @examples
#' # Alphabetically, Alaska is the second state...
#' print(state.abb[2])
#'
.DownloadActiveSitesForState <- function(state, config){
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=",
               state,
               "&period=",
               config$collections$lookback,
               "&siteOutput=expanded&hasDataTypeCd=iv,id",
               sep = "")
  
  active <- dataRetrieval::importRDB1(url)
  RPostgreSQL::dbWriteTable(conn = conn2, 
                            name = config$tables$active.sites, 
                            value = active, 
                            append = TRUE, 
                            row.names = FALSE, 
                            overwrite = FALSE)  
}

#' Downloads Assets at Sites
#' 
#' Downloads & imports the inventory of instantaneous values at a given set of sites. 
#' 
#' @param sites A character vector of USGS site numbers, comma separated, with no spaces. (e.g. "0101010,1020202,10101010")
#' @param config Configuration object created by LoadConfiguration.
#' @seealso To build a \code{config} object, see \code{\link{LoadConfiguration}}.
.DownloadAssetsForSites <- function(sites, config){
  
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb,1.0&sites=",
               sites,
               "&seriesCatalogOutput=true&outputDataTypeCd=iv",
               sep = "")
  
  assets <- dataRetrieval::importRDB1(url)
  assets <- base::transform(assets, 
                            seriesid = paste(agency_cd, ":", site_no, ":", parm_cd, ":00011:", 
                                             formatC(dd_nu, width = 5, format = "d", flag = "0"), sep = ""),
                            familyid = paste(agency_cd, ":", site_no, ":00011:", 
                                             formatC(dd_nu, width = 5, format = "d", flag = "0"), sep = ""))
  
  if(nrow(assets) > 0){
    RPostgreSQL::dbWriteTable(conn = conn2, 
                              name = config$tables$site.assets, 
                              value = assets, 
                              append = TRUE, 
                              row.names = FALSE, 
                              overwrite = FALSE)
  }
}


# @ return primary key
.StageURL <- function(config = config, url = url) {
  
  query <- paste("INSERT INTO ", 
                 config$tables$url.stash,
                 "(url) values ('",
                 url,
                 "') RETURNING id;",
                 sep = "")
  
  cc <- RunQuery(conn = conn2, 
                 query = query,
                 quietly = TRUE,
                 config = config)
  
}

.UnstageURL <- function(config = config, url = NULL, id = NULL) {
  if (is.null(url) && is.null(id)){
    .stop("Error attempting to unstage URL. Must specify either url or id.",
          config = config)
  } 
  
  if (is.null(url)){
    
    query <- paste("delete from only ",
                   config$tables$url.stash,
                   " where id = ",
                   id,
                   ";",
                   sep = "")
    
  } else {
    
    query <- paste("delete from only ",
                   config$tables$url.stash,
                   "where url = '",
                   url,
                   "';",
                   sep = "")
    
  }
  
  cc <- RunQuery(conn = conn2, 
                 query = query, 
                 config = config)
  
}


.RetryDownloadDataFromNWIS <- function(config, url) {
  .DownloadDataFromNWIS(sites = NULL, params = NULL, url = url, config = config)
}

.DownloadDataFromNWIS <- function(sites, 
                                  params, 
                                  startDate = NULL, 
                                  endDate = NULL, 
                                  period = NULL, 
                                  offset = NULL,
                                  url = NULL,
                                  stage = FALSE,
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
  
  # id <- .StageURL(url = url, config = config)
  
  # .message(paste("Staged", url, "with id", id),
  #           config = config)
  
 g <- RCurl::basicTextGatherer()
  
  for(i in 1:5) {
    t <- tryCatch({
      
     responseCode <- RCurl::curlPerform(url = url, 
                                        writefunction = g$update, 
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
    #.message("Successful download.", config = config)
  } else {
    .warning(paste0("Url failed permanently: ",
                    url,
                    "."),
             config = config)
    
    return(NULL)
  }
  
  t <- tryCatch({
    doc <- XML::xmlTreeParse(g$value(), getDTD = FALSE, useInternalNodes = TRUE) 
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
  
  
 vars <- XML::xpathApply(doc, "//ns1:timeSeries") 
  
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
            
            table <- ifelse(stage, paste0(config$tables$staging, startDate), config$tables$data)
            
            cc <- RPostgreSQL::dbWriteTable(conn2, 
                                            name = table, 
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
  #  .UnstageURL(id = id, config = config)
}
