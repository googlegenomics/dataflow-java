#!/bin/bash
# Runs CountReads pipeline locally and in the cloud (pass "cloud" as a param).
# Assumes the script is run from dataflow-java directory.
# Assumes client_secrets.json is located in a parent directory of dataflow-java.

JAR=target/google-genomics-dataflow*.jar
CLIENT_SECRETS=$(readlink -f ../client_secrets.json)
# Assumes the following variables are set
# Please set them before calling or edit this file and set them.
# PROJECT_ID - id of your cloud project
# OUTPUT - GCS path for output file
# STAGING - GCS path for staging files
# DATASET_ID - Id of genomics API data set (only needed if using API)
# READGROUPSET_ID - Id of genomics AP readgroup set (only needed if using API)
# DESIRED_CONTIGS - reference:start:end[,reference:start:end]
# BAM_FILE_PATH - GCS path to BAM file (only needed if using BAM file input)
# 
# Example call for API reading
# export PROJECT_ID=your-project-id
# export OUTPUT=gs://test/df/count_reads/output/count.txt
# export STAGING=gs://test/df/count_reads/staging
# export DATASET_ID=15448427866823121459
# export READGROUPSET_ID=CK256frpGBD44IWHwLP22R4
# export DESIRED_CONTIGS=seq1:0:800
# # Local run: 
# src/main/scripts/count_reads.sh
# # Cloud run: 
# src/main/scripts/count_reads.sh cloud
#
# Example call for BAM reading
# export PROJECT_ID=your-project-id
# export OUTPUT=gs://test/df/count_reads/output/count.txt
# export STAGING=gs://test/df/count_reads/staging
# export DESIRED_CONTIGS=seq1:0:800
# BAM_FILE_PATH=gs://test/df/NA12878.chrom20.ILLUMINA.bwa.CEU.exome.20121211.bam
# # Local run: 
# src/main/scripts/count_reads.sh bam
# # Cloud run: 
# src/main/scripts/count_reads.sh bam cloud


if [ "$1" = "bam" ]; then
 bam_argument="--BAMFilePath=$BAM_FILE_PATH"
else
 api_argument="--datasetId=$DATASET_ID --readGroupSetId=$READGROUPSET_ID"
fi
if [ "$2" = "cloud" ]; then
  additional_arguments="--stagingLocation=${STAGING} --numWorkers=2 --runner=BlockingDataflowPipelineRunner"
else
  additional_arguments="--numWorkers=1"
fi

echo Running: $JAR
echo Client secrets in: $CLIENT_SECRETS
echo Output in: $OUTPUT
echo Additional arguments: $additional_arguments

java -XX:MaxPermSize=512m -Xms2g -Xmx3g -cp $JAR \
com.google.cloud.genomics.dataflow.pipelines.CountReads \
--project=$PROJECT_ID \
--output=$OUTPUT \
--genomicsSecretsFile=$CLIENT_SECRETS \
--references=$DESIRED_CONTIGS \
$bam_argument $api_argument $additional_arguments


