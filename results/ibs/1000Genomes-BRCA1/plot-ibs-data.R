
## ----echo=FALSE, eval=FALSE----------------------------------------------
## # This codelab assumes that the current working directory is where the Rmd file
## # resides
## setwd("/YOUR/PATH/TO//1000Genomes-BRCA1/ibs")


## ----message=FALSE, comment=NA-------------------------------------------
library(reshape2)
ibs_pairs <- read.table("./1000genomes_phase1_brca1.ibs", header=FALSE,
                       stringsAsFactors=FALSE)
colnames(ibs_pairs) <- c("sample1", "sample2", "ibs_score")


## ----ibs-heat-map, fig.align="center", fig.width=10, message=FALSE, comment=NA----
require(ggplot2)
p <- ggplot(data=ibs_pairs, aes(x=sample1, y=sample2)) +
     theme(axis.ticks=element_blank(), axis.text=element_blank()) +
     geom_tile(aes(fill=ibs_score), colour="white") +
     scale_fill_gradient(low="white", high="steelblue",
                         guide=guide_colourbar(title="IBS Score")) +
     labs(list(title="IBS Heat Map", x="Sample", y="Sample"))
p


