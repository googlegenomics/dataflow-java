# Identity By State (IBS) for 1000 Genomes BRCA1



The input IBS matrix is an $N^2\times 3$ matrix, where $N$ is the size of the
population and each row represents the IBS score for a pair of individuals.


```r
library(reshape2)
ibs_pairs <- read.table(file=ibs_filename, stringsAsFactors=FALSE)
colnames(ibs_pairs) <- c("sample1", "sample2", "ibs_score")
```

Make the IBS matrix symmetric.


```r
ibs_pairs_mirrored <- data.frame(sample1=ibs_pairs$sample2,
                                 sample2=ibs_pairs$sample1,
                                 ibs_score=ibs_pairs$ibs_score)
ibs_pairs <- rbind(ibs_pairs, ibs_pairs_mirrored)
write.table(ibs_pairs, file=symmetric_ibs_filename)
```

Extract the IBS matrix for a random sample of the individuals.


```r
individuals <- unique(ibs_pairs$sample1)
sample_size <- 50
sample <- sample(individuals, sample_size)
ibs_pairs <- subset(ibs_pairs, ibs_pairs$sample1 %in% sample)
ibs_pairs <- subset(ibs_pairs, ibs_pairs$sample2 %in% sample)
```

Draw a heat map based on the IBS scores.


```r
require(ggplot2)
p <- ggplot(data=ibs_pairs, aes(x=sample1, y=sample2)) +
     theme(axis.ticks=element_blank(), axis.text=element_blank()) +
     geom_tile(aes(fill=ibs_score), colour="white") +
     scale_fill_gradient(low="white", high="steelblue", na.value="black",
                         guide=guide_colourbar(title="IBS Score")) +
     labs(list(title="Identity By State (IBS) Heat Map", x="Sample", y="Sample"))
p
```

<img src="figure/ibs-heat-map-1.png" title="plot of chunk ibs-heat-map" alt="plot of chunk ibs-heat-map" style="display: block; margin: auto;" />
