#!/usr/bin/env Rscript

library("rmarkdown")
library("knitr")

# Generates the files for the given Rmd file in the designated directory.
generate_output <- function(rmd_file, dir_name) {
  original_wd <- getwd()
  dir.create(dir_name)
  setwd(dir_name)
  purl(rmd_file, documentation=1)
  knit(rmd_file)
  render(rmd_file, output_dir=dir_name)
  setwd(original_wd)
}

check_file_exists <- function(filename) {
  if (file.exists(filename) == FALSE) {
    stop(paste(filename, "does not exist.", sep=" "))
  }
}

symmetric_filename <- function(filename)  {
  symmetric_basename <- paste("symmetric", basename(filename), sep="-")
  return (normalizePath(paste(dirname(filename), symmetric_basename, sep="/")))
}

# The command line arguments are the names of the two IBS files to compare.
#
# The output directories of this script include the directories in which the
# input files reside.
args <- commandArgs(TRUE)

plot_rmd_file <- normalizePath("plot-ibs-data.rmd")
check_file_exists(plot_rmd_file)

compare_rmd_file <- normalizePath("compare-ibs-data.rmd")
check_file_exists(compare_rmd_file)

ibs_filename1 <- normalizePath(args[1])
check_file_exists(ibs_filename1)

ibs_filename2 <- normalizePath(args[2])
check_file_exists(ibs_filename2)

ibs_filename <- ibs_filename1
symmetric_ibs_filename <- symmetric_filename(ibs_filename1)
generate_output(rmd_file=plot_rmd_file, dir_name=dirname(ibs_filename1))
check_file_exists(symmetric_ibs_filename)

ibs_filename <- ibs_filename2
symmetric_ibs_filename <- symmetric_filename(ibs_filename2)
generate_output(rmd_file=plot_rmd_file, dir_name=dirname(ibs_filename2))
check_file_exists(symmetric_ibs_filename)

ibs_filename1 <- symmetric_filename(ibs_filename1)
ibs_filename2 <- symmetric_filename(ibs_filename2)
generate_output(rmd_file=compare_rmd_file, dir_name="compare")

