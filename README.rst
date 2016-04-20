==============
dataflow-java |Build Status|_ |Build Coverage|_
==============

.. |Build Status| image:: http://img.shields.io/travis/googlegenomics/dataflow-java.svg?style=flat
.. _Build Status: https://travis-ci.org/googlegenomics/dataflow-java

.. |Build Coverage| image:: http://img.shields.io/coveralls/googlegenomics/dataflow-java.svg?style=flat
.. _Build Coverage: https://coveralls.io/r/googlegenomics/dataflow-java?branch=master

 If you are ready to start coding, take a look at the information below.  But if you are
 looking for a task-oriented list (e.g., `How do I compute principal coordinate analysis
 with Google Genomics? <http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html>`_),
 a better place to start is the `Google Genomics Cookbook <http://googlegenomics.readthedocs.org/en/latest/index.html>`_ .

Getting started
---------------

#. First git clone this repository.

#. If you have not already done so, follow the Google Genomics `getting started instructions <https://cloud.google.com/genomics/install-genomics-tools>`_ to set up your environment including `installing gcloud <https://cloud.google.com/sdk/>`_ and running ``gcloud init``.

#. If you have not already done so, follow the Google Cloud Dataflow `getting started instructions <https://cloud.google.com/dataflow/getting-started>`_ to set up your environment for Dataflow.

#. This project now includes code for calling the Genomics API using `gRPC <http://www.grpc.io>`_.  To use gRPC, you'll need a version of ALPN that matches your JRE version. 

 #. See the `ALPN documentation <http://www.eclipse.org/jetty/documentation/9.2.10.v20150310/alpn-chapter.html>`_ for a table of which ALPN jar to use for your JRE version.
 #. Then download the correct version from `here <http://mvnrepository.com/artifact/org.mortbay.jetty.alpn/alpn-boot>`_.

Local Run
---------
To use this code, build the client using `Apache Maven`_::

    cd dataflow-java
    mvn package

Then you can run a pipeline locally with the command line, passing in the Project ID and Google Cloud Storage bucket you made in the first step.  This command runs the VariantSimilarity pipeline (which runs PCoA on a dataset)::

    java -Xbootclasspath/p:/YOUR/PATH/TO/alpn-boot-YOUR-VERSION.jar \
      -cp target/google-genomics-dataflow*-runnable.jar \
      com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
      --variantSetId=3049512673186936334 \
      --references=chr17:41196311:41277499 \
      --output=gs://your-bucket/output/localtest.txt

Run on Google Compute Engine
----------------------------
To deploy your pipeline (which runs on Google Compute Engine), ALPN is no longer needed but some additional command line arguments are required::

    java -cp target/google-genomics-dataflow*-runnable.jar \
      com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity \
      --project=your-project-id \
      --variantSetId=3049512673186936334 \
      --references=chr17:41196311:41277499 \
      --output=gs://your-bucket/output/test.txt \
      --runner=BlockingDataflowPipelineRunner \
      --project=your-project-id \
      --stagingLocation=gs://your-bucket/staging \
      --numWorkers=1

**See the** `Google Genomics Cookbook <http://googlegenomics.readthedocs.org/>`_ **for more sample command lines for the various pipelines.**

.. _Apache Maven: http://maven.apache.org/download.cgi

Command Line Options
--------------------

Use ``--help`` to get more information about the command line options.  Change
the pipeline class name below to match the one you would like to run::

  java -cp google-genomics-dataflow*-runnable.jar \
    com.google.cloud.genomics.dataflow.pipelines.VariantSimilarity --help

Code layout
-----------

The `Main code directory </src/main/java/com/google/cloud/genomics/dataflow>`_
contains several useful utilities:

coders:
  includes ``Coder`` classes that are useful for Genomics pipelines.

functions:
  and its subdirectories contain common DoFns and other serializable function types that can be reused as part of any pipeline. ``OutputPCoAFile`` is an example of a complex ``PTransform`` that provides a useful common analysis.

model:
  pojos for intermediate data types used in the sample pipelines.  If you are instead looking for the Reads and Variants java objects:
  
  * REST API `javadocs <https://developers.google.com/resources/api-libraries/documentation/genomics/v1/java/latest/overview-summary.html>`_
  * gRPC API `protobuf descriptors <https://github.com/googlegenomics/utils-java/tree/master/src/main/proto/google>`_
  
pipelines:
  contains example pipelines which demonstrate how Google Cloud Dataflow can work with Google Genomics

  * ``VariantSimilarity`` runs a principal coordinates analysis over a dataset containing variants, and
    writes a file of graph results that can be easily displayed by Google Sheets.

  * ``IdentityByState`` runs IBS over a dataset containing variants. See the `results/ibs <results/ibs>`_
    directory for more information on how to use the pipeline's results.

  * and several others!

readers:
  contains functions that perform API calls to read data from the genomics API and also from BAM files in Cloud Storage

writers:
  contains functions that perform API calls to write data to BAM files in Cloud Storage

utils:
  contains utilities for running dataflow workflows against the genomics API

  * ``GenomicsOptions.java``, ``ShardOptions`` and ``GCSOutputOptions``
    use these classes for your command line options to take advantage of common command
    line functionality

Maven artifact
--------------
This code is also deployed as Maven artifacts through Sonatype, including both a normal jar and a runnable jar containing all dependencies (a fat jar). The
`utils-java readme <https://github.com/googlegenomics/utils-java#releasing-new-versions>`_
has detailed instructions on how to deploy new versions.

To depend on this code, add the following to your ``pom.xml`` file::

  <project>
    <dependencies>
      <dependency>
        <groupId>com.google.cloud.genomics</groupId>
        <artifactId>google-genomics-dataflow</artifactId>
        <version>LATEST</version>
      </dependency>
    </dependencies>
  </project>

You can find the latest version in
`Maven's central repository <https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22google-genomics-dataflow%22>`_

For an example pipeline that depends on this code in another GitHub repository, see https://github.com/googlegenomics/codelabs/tree/master/Java/PlatinumGenomes-variant-transformation.

Local development
-----------------

With Maven you can locally install a SNAPSHOT version of this code (or any of its dependencies), to use from other projects 
directly without having to wait for the Maven repository. Use:

``mvn install``

to run the full tests and do a local install. You can also use

``mvn install -DskipITs``

to run only the unit tests and do a local install. This is faster.

Project status
--------------

Goals
~~~~~
* Provide a Maven artifact which makes it easier to use Google Genomics within Google Cloud Dataflow.
* Provide some example pipelines which demonstrate how Dataflow can be used to analyze Genomics data.

Current status
~~~~~~~~~~~~~~
This code is in active development.  See the github issues for more detail.
