# This file is released under the MIT License.
#
# It was derived from the rlogging package by Maarten-Jan Kallen 
# which is available here: https://github.com/mjkallen/rlogging. 
# 
# Modified by Kevin M. Smith to be bundled with the nwisnfie 
# package and use a configuration file rather than an environment.
#
# The MIT license is copied below.
#
# The MIT License (MIT)
#
# Copyright (c) 2013 Maarten-Jan Kallen
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#   
#   The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# 

.PrintLogMessage <- function(..., config, domain = NULL, level) {
  
  timestamp <- format(Sys.time(), format = config$logging$time.stamp.format)
  base::message(timestamp, ..., domain=  domain)
  
  if (!is.null(config$logging$level)) {
    cat(timestamp, ..., "\n", file = config$logging$file, sep="", append=TRUE)
  }
  
}

.message <- function(..., config, domain = NULL, appendLF = TRUE) {
  args <- list(...)
  is.condition <- length(args) == 1L && inherits(args[[1L]], "condition")
  if (is.condition) {
    # bypass the logger if a condition is supplied or if loglevel is set to "NONE"
    base::message(..., domain = domain, appendLF = appendLF)
  } else {
    # if loglevel is set to INFO, then print log message, else do nothing
    
    if (config$logging$level == "INFO") {
      .PrintLogMessage("[INFO] ", config = config, ...)
    }
  }
  invisible()
}

.stop <- function(..., config, call. = TRUE, domain = NULL) {
  args <- list(...)
  is.condition <- length(args) == 1L && inherits(args[[1L]], "condition")
  if (!is.condition) {
    .PrintLogMessage("[STOP] ", config = config, ...)
  }
  base::stop(..., call. = call., domain = domain)
  invisible()
}

.warning <- function(..., config, call. = TRUE, immediate. = FALSE, domain = NULL) {
  args <- list(...)
  is.condition <- length(args) == 1L && inherits(args[[1L]], "condition")
  if (!is.condition) {
    # if loglevel is set to INFO or WARN, then print log message
    if (config$logging$level %in% c("INFO", "WARN")) {
      .PrintLogMessage("[WARN] ", config = config, ...)
    }
    # always collect warnings when printing log messages
    immediate. <- FALSE
  }
  # always call the base warning function to collect warnings
  base::warning(..., call. = call., immediate. = immediate., domain = domain)
  invisible()
}
