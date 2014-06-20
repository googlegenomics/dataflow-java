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
    com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
    --project=google.com:genomics-api \
    --output=<gs://mybucket/VariantSimilarityLocal.txt>
``` 
    
Run deployed

```
  java -cp target/googlegenomics-dataflow-java-v1beta.jar \
    com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
    --runner=BlockingDataflowPipelineRunner \
    --project=google.com:genomics-api \
    --stagingLocation=gs://cloud-genomics-dataflow-tests/staging \
    --output=gs://cloud-genomics-dataflow-tests/output/test.txt \
    --numWorkers=10 \
    --zone=us-central1-b
```    

By default, the max workers you can have without requesting more quota is 16. (That's the limit on GCE VMs)
