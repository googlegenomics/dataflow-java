dataflow-java
=============

**This project is confidential. Do not share with anyone not in the Dataflow TT program**


Getting started
---------------

* To use, first build the client using `Apache Maven`_::

    cd dataflow-java
    mvn compile
    mvn bundle:bundle

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

* Deploying FDA Kmer indexing pipeline::
  
     java -Xmx8g -cp "target/googlegenomics-dataflow-java-v1beta.jar" \
      com.google.cloud.genomics.dataflow.pipelines.FDAPipeline \
      --runner=BlockingDataflowPipelineRunner \
      --project=google.com:genomics-api \
      --stagingLocation=gs://cloud-genomics-user-peijinz/staging \
      --outDir=gs://cloud-genomics-user-peijinz/output/test \
      --datasetId=13770895782338053201 \
      --kValues=4,5,6 \
      --writeKmer \
      --apiKey="AIzaSyB6DSNuXBIzt4PaB9GNchRy6nZo5PdGhPI" \
      --zone=us-central1-b \
      --numWorkers=50

datasetId:
  Dataset to query over. Currently available:
  
  - 13770895782338053201: Test Dataset (contains one copy of SRR1188432)
  - 13548522727457381097: Listeria Dataset
  - 2831627299882627465: Salmonella Dataset

kValues:
  Values of k to use for indexing. Multiple values supported.
  Note: dataflow currently has issues going over k >= 8

writeKmer:
  Option for writing kmer table out to file

numWorkers:
  Number of workers to run workflow over. Max value is 100.

.. _Apache Maven: http://maven.apache.org/download.cgi


Code layout
-----------

The `Main code directory </src/main/java/com/google/cloud/genomics/dataflow>`_ contains several useful utilities:

coders: 
  includes ``Coder`` classes that are useful for Genomics pipelines. ``GenericJsonCoder`` 
  can be used with any of the Java client library classes (like ``Read``, ``Variant``, etc)
  
functions:
  contains common ParDo and SeqDo functions that can be reused as part of any pipeline. 
  ``OutputPCoAFile`` is an example of a complex `PTransform` that provides a useful common analysis.
  
pipelines:
  There are currently 2 example pipelines that are used to demonstrate how Google Cloud Dataflow 
  can work with Google Genomics. 
  
  * ``VariantSimilarity`` runs a Principal coordinates analysis over a dataset containing variants, and 
    writes a file of graph results that can be easily displayed by Google Sheets.

  * ``FDAPipeline`` generates kmers for a dataset of reads, and runs Principal coordinates 
    analysis over the resulting data to find readset outliers that may indicate new species. 

DataflowWorkarounds.java:
  contains workarounds needed to use the Google Cloud Dataflow APIs. 
  This class should dissapear before Dataflow goes public.

GenomicsApi.java:
  use this class to call the Genomics API - it contains many useful utilities to make API handling easier.

GenomicsOptions.java:
  extend this class for your command line options to make handling authorization 
  for the Genomics APIs a bit easier.


Project status
--------------

Goals
~~~~~
* Provide a Maven artifact which makes it easier to use Google Genomics within Google Cloud Dataflow.
* Provide some example pipelines which demonstrate how Dataflow can be used to analyze Genomics data.

Current status
~~~~~~~~~~~~~~
This code is in active development, it will be deployed to Maven once Dataflow is.
