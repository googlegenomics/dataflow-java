dataflow-java
=============

##Note: this project is confidential. Do not share with anyone not in the Dataflow TT program


Getting started
---------------

* To use, first build the client using `Apache Maven`_::

  cd dataflow-java
  mvn compile
  mvn bundle:bundle

.. _Apache Maven: http://maven.apache.org/download.cgi

* Then you can run a pipeline locally with the command line::

  java -cp target/googlegenomics-dataflow-java-v1beta.jar \
    com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
    --project=google.com:genomics-api \
    --output=gs://cloud-genomics-dataflow-tests/output/localtest.txt
    
  Note: when running locally, you may run into memory issues depending on the capacity of your local machine.
  
* To deploy your pipeline (which runs on Google Compute Engine), some additional 
  command line arguments are required::

  java -cp target/googlegenomics-dataflow-java-v1beta.jar \
    com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
    --runner=BlockingDataflowPipelineRunner \
    --project=google.com:genomics-api \
    --stagingLocation=gs://cloud-genomics-dataflow-tests/staging \
    --output=gs://cloud-genomics-dataflow-tests/output/test.txt \
    --numWorkers=10 \
    --zone=us-central1-b

  Note: By default, the max workers you can have without requesting more GCE quota 
  is 16. (That's the default limit on VMs)

TODO: Explain each command line arg so this section makes more sense


Code layout
-----------

The [Main directory](/src/main/java/com/google/cloud/genomics/dataflow) contains several useful utilities:

