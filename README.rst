dataflow-java
=============

**This project is confidential. Do not share with anyone not in the Dataflow TT program**


Getting started
---------------

* First, follow the Google Cloud Dataflow `getting started instructions
  <https://cloud.google.com/dataflow/java-sdk/getting-started>`_ to set up your environment
  for Dataflow. You will need your Project ID and Google Cloud Storage bucket in the following steps.

* To use this code, build the client using `Apache Maven`_::

    cd dataflow-java
    mvn compile
    mvn bundle:bundle

* Then, follow the Google Genomics `sign up instructions`_ to generate a valid
  ``client_secrets.json`` file.

* Move the ``client_secrets.json`` file into the dataflow-java directory.
  (Authentication will take place the first time you run a pipeline.)

* Then you can run a pipeline locally with the command line, passing in the
  Project ID and Google Cloud Storage bucket you made in the first step::

    java -cp target/googlegenomics-dataflow-java-v1beta2.jar \
      com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
      --project=my-project-id \
      --output=gs://my-bucket/output/localtest.txt \
      --genomicsSecretsFile=client_secrets.json
    
  Note: when running locally, you may run into memory issues depending on the
  capacity of your local machine.
  
* To deploy your pipeline (which runs on Google Compute Engine), some additional 
  command line arguments are required::

    java -cp target/googlegenomics-dataflow-java-v1beta2.jar \
      com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
      --runner=BlockingDataflowPipelineRunner \
      --project=my-project-id \
      --stagingLocation=gs://my-bucket/staging \
      --output=gs://my-bucket/output/test.txt \
      --genomicsSecretsFile=client_secrets.json \
      --numWorkers=10

  Note: By default, the max workers you can have without requesting more GCE quota 
  is 16. (That's the default limit on VMs)

.. _Apache Maven: http://maven.apache.org/download.cgi
.. _sign up instructions: https://cloud.google.com/genomics


Code layout
-----------

The `Main code directory </src/main/java/com/google/cloud/genomics/dataflow>`_
contains several useful utilities:

coders: 
  includes ``Coder`` classes that are useful for Genomics pipelines. ``GenericJsonCoder`` 
  can be used with any of the Java client library classes (like ``Read``, ``Variant``, etc)
  
functions:
  contains common ParDo and SeqDo functions that can be reused as part of any pipeline. 
  ``OutputPCoAFile`` is an example of a complex ``PTransform`` that provides a useful common analysis.
  
pipelines:
  contains example pipelines which demonstrate how Google Cloud Dataflow can work with Google Genomics
  
  * ``VariantSimilarity`` runs a principal coordinates analysis over a dataset containing variants, and
    writes a file of graph results that can be easily displayed by Google Sheets.

readers:
  contains functions that perform API calls to read data from the genomics API

utils: 
  contains utilities for running dataflow workflows against the genomics API
  
  * ``DataflowWorkarounds``
    contains workarounds needed to use the Google Cloud Dataflow APIs. 
    This class should dissapear before Dataflow goes public.

  * ``GenomicsAuth.java``
    Use this class for performing authentication when calling the API. It allows for using either 
    an api key or client secrets file.

  * ``GenomicsOptions.java`` and ``GenomicsDatasetOptions``
    extend these class for your command line options to take advantage of common command
    line functionality


Project status
--------------

Goals
~~~~~
* Provide a Maven artifact which makes it easier to use Google Genomics within Google Cloud Dataflow.
* Provide some example pipelines which demonstrate how Dataflow can be used to analyze Genomics data.

Current status
~~~~~~~~~~~~~~
This code is in active development, it will be deployed to Maven once Dataflow is.

* TODO: Explain all the possible command line args:``zone``, ``allContigs``, etc
* TODO: clean up the pipeline warnings - especially the silly reflections ones
* TODO: Setup Travis integration once this repo is public
