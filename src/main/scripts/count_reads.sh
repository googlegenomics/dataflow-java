#!/bin/bash
# Runs CountReads pipeline locally and in the cloud (pass "cloud" as a param).
# Assumes the script is run from dataflow-java directory.
# Assumes client_secrets.json is located in a parent directory of dataflow-java.

JAR=target/google-genomics-dataflow-v1beta2-0.2-SNAPSHOT.jar
CLIENT_SECRETS=$(readlink -f ../client_secrets.json)
# Assumes the following variables are set
# Please set them before calling or edit this file and set them.
# PROJECT_ID=cloud-project-name
# OUTPUT=gs://test/df/count_reads/output/count.txt
# STAGING=gs://test/df/count_reads/staging
# DATASET_ID=15448427866823121459
# READGROUPSET_ID=CK256frpGBD44IWHwLP22R4
# DESIRED_CONTIGS=20:56311809:62603264
# BAM_FILE_PATH=gs://test/NA12878.chrom20.ILLUMINA.bwa.CEU.exome.20121211.bam

if [ "$1" = "bam" ]; then
 bam_argument="--BAMFilePath=$BAM_FILE_PATH"
fi
if [ "$2" = "cloud" ]; then
  additional_arguments="--stagingLocation=${STAGING} --numWorkers=1 --runner=BlockingDataflowPipelineRunner"
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
--datasetId=$DATASET_ID \
--readGroupSetId=$READGROUPSET_ID \
--references=$DESIRED_CONTIGS \
$additional_arguments \
$bam_argument
