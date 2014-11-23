#!/usr/bin/env Rscript

library("rmarkdown")
library("knitr")

rmdfile="plot-ibs-data.rmd"

render(rmdfile)
knit(rmdfile)
purl(rmdfile, documentation=1)

