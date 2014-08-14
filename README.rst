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
  contains example pipelines which demonstrate how Cloud Dataflow can work with Google Genomics
  
  * ``VariantSimilarity`` runs a Principal coordinates analysis over a dataset containing variants, and 
    writes a file of graph results that can be easily displayed by Google Sheets.
    
readers:
  contains functions that perform API calls to read data from the genomics API

utils: 
  contains utilities for running dataflow workflows against the genomics API
  
  * ``DataflowWorkarounds``:
    contains workarounds needed to use the Google Cloud Dataflow APIs. 
    This class should dissapear before Dataflow goes public.

  * ``GenomicsAuth.java``
    Use this class for performing authentication when calling the API. It allows for using either 
    an api key or client secrets file.

  * ``GenomicsOptions.java:
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
