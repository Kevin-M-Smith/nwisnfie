% Generated by roxygen2 (4.1.0): do not edit by hand
% Please edit documentation in R/install.R
\name{InstallFiles}
\alias{InstallFiles}
\title{Installs scripts and configuration files to the specified directory.}
\usage{
InstallFiles(installDirectory, overwrite = FALSE)
}
\arguments{
\item{installDirectory}{Directory to to}

\item{overwrite}{Overwrite previously installed files?}
}
\description{
If the directory does not exist, one will be created.
An error will be thrown if previous installation exists in the directory, unless overwrite == TRUE.
}

