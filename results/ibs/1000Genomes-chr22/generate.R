#!/usr/bin/env Rscript

library("rmarkdown")
library("knitr")

rmdFile <- "analyze-ibs-data.Rmd"
purl(rmdFile, documentation=1)
knit(rmdFile)
render(rmdFile)

