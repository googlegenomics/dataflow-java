dataflow-java
=============

##This project is private. These docs are bad on purpose. 

[Main directory](/src/main/java/com/google/cloud/genomics/dataflow)

```
cd dataflow-java
mvn compile
mvn bundle:bundle
```

Run locally
Note: when running locally, you can only process about 1000 base positions before exceeding memory.

```
java -cp target/googlegenomics-dataflow-java-v1beta.jar \
    com.google.cloud.genomics.dataflow.VariantSimilarity \
    --project=<PROJECT_ID> \
    --output=<gs://mybucket/VariantSimilarityLocal.txt>
``` 
    
Run deployed

```
  java -cp target/googlegenomics-dataflow-java-v1beta.jar \
    com.google.cloud.genomics.dataflow.VariantSimilarity \
    --runner=BlockingDataflowPipelineRunner \
    --project=<PROJECT_ID> \
    --stagingLocation=gs://mybucket/staging \
    --output=gs://mybucket/VariantSimilarity.txt \
    --numWorkers=16
```    

The max workers you can have without requesting more quota is 16. (That's the limit on GCE VMs)
