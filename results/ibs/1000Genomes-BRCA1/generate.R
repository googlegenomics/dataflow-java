#!/usr/bin/env Rscript

library("rmarkdown")
library("knitr")

GenerateOutput <- function(rmdFile, dirName) {
  # Generates the files for the given Rmd file in the designated directory.
  #
  # Args:
  #  rmdFile: The .Rmd file to process.
  #  dirName: The name of the output directory.
  originalWD <- getwd()
  dir.create(dirName)
  setwd(dirName)
  purl(rmdFile, documentation=1)
  knit(rmdFile)
  render(rmdFile, output_dir=dirName)
  setwd(originalWD)
}

CheckFileExists <- function(filename) {
  if (file.exists(filename) == FALSE) {
    stop(paste(filename, "does not exist.", sep=" "))
  }
}

SymmetricFilename <- function(filename)  {
  symmetricBasename <- paste("symmetric", basename(filename), sep="-")
  return (normalizePath(paste(dirname(filename), symmetricBasename, sep="/")))
}

# The command line arguments are the names of the two IBS files to compare.
#
# The output directories of this script include the directories in which the
# input files reside.
args <- commandArgs(TRUE)

plotRmdFile <- normalizePath("plot-ibs-data.rmd")
CheckFileExists(plotRmdFile)

compareRmdFile <- normalizePath("compare-ibs-data.rmd")
CheckFileExists(compareRmdFile)

ibsFilename1 <- normalizePath(args[1])
CheckFileExists(ibsFilename1)

ibsFilename2 <- normalizePath(args[2])
CheckFileExists(ibsFilename2)

ibsFilename <- ibsFilename1
symmetricIBSFilename <- SymmetricFilename(ibsFilename1)
GenerateOutput(rmdFile=plotRmdFile, dirName=dirname(ibsFilename1))
CheckFileExists(symmetricIBSFilename)

ibsFilename <- ibsFilename2
symmetricIBSFilename <- SymmetricFilename(ibsFilename2)
GenerateOutput(rmdFile=plotRmdFile, dirName=dirname(ibsFilename2))
CheckFileExists(symmetricIBSFilename)

ibsFilename1 <- SymmetricFilename(ibsFilename1)
ibsFilename2 <- SymmetricFilename(ibsFilename2)
GenerateOutput(rmdFile=compareRmdFile, dirName="compare")

