#!/bin/bash

# Tests the count_reads pipeline in the cloud and locally.
# Returns 0 only if tests pass. This takes a little while.

# For this to work you must 
# (1) run this script from dataflow-java
# (2) have a client-secrets.json file in the parent folder 
#     (or client_secrets.json)
# (3) have gsutil in your path, and
# (4) Set these environment variables:
#     PROJECT_ID - id of your cloud project
#     STAGING - GCS folder for staging files
#     GS_TEST_BAM - GCS input BAM file
#     GS_TEST_DATASET - DatasetID for test input
#     GS_TEST_READGROUPSET - ReadGroupsetID for test input
#     OUTPUT - GCS output file

checkOutput () {
  if [ ! -r "$OUT" ]; then
    echo "FAILED: output file $OUT missing.";
    exit 1
  fi
  # fail if the output suggests something went wrong
  # (This is a little bit fragile, but we kind of have to do this
  #  because currently Pipeline doesn't return an error code
  #  if there's a problem: instead, stuff is printed to the screen.)
  for WORD in exception FAIL Error; do  
    if grep -i $WORD $OUT; then 
      echo "FAILED: client output mentions '$WORD'";
      echo "Check ${OUT}."
      exit 1
    fi
  done
  # fail the test if no output is present
  if ! gsutil stat "${OUTPUT}*" > /dev/null; then
    echo "FAILED: output file ${OUTPUT} missing";
    exit 1
  fi
}

# exit the script if any command fails
set -o errexit

# 1. Check prerequisites

if [ -r "../client-secrets.json" ]; then
   CLIENT_SECRETS=$(readlink -f ../client-secrets.json)
else
   CLIENT_SECRETS=$(readlink -f ../client_secrets.json)
fi
if [ ! -r "${CLIENT_SECRETS}" ]; then
  echo "file missing: ../client-secrets.json";
  exit 1
fi
if [ -z "${PROJECT_ID}" ]; then 
  echo "must set PROJECT_ID"; 
  exit 1
fi
if [ -z "${STAGING}" ]; then 
  echo "must set STAGING"; 
  exit 1
fi
if [ -z "${OUTPUT}" ]; then 
  echo "must set OUTPUT"; 
  exit 1
fi
if ! gsutil --version > /dev/null; then
  echo "gsutil not found.";
  exit 1
fi
mkdir -p tmp
TEMP="tmp"

JAR=$(ls -v target/google-genomics*jar | tail -n 1)

echo "Tests for the CountReads pipeline, using:"
echo "JAR:        $JAR"
echo "secrets:    $CLIENT_SECRETS"
echo "PROJECT_ID: $PROJECT_ID"
echo "STAGING:    $STAGING"
echo "OUTPUT:     $OUTPUT"
echo "TEMP FLDR:  $TEMP"
echo "INPUT:      $GS_TEST_BAM"

# 2. test local execution

OUT=$TEMP/local-out.txt
CONTIGS=seq1:0:800
rm -f $OUT
# the || is to continue even if the file isn't there
gsutil rm "${OUTPUT}-000*" || echo ""
echo "Testing local execution  (output in $OUT)."

if ! java -XX:MaxPermSize=512m -Xms2g -Xmx3g -cp $JAR \
  com.google.cloud.genomics.dataflow.pipelines.CountReads \
  --project=$PROJECT_ID \
  --output=$OUTPUT \
  --genomicsSecretsFile=$CLIENT_SECRETS \
  --references=$CONTIGS \
  --BAMFilePath=$GS_TEST_BAM > $OUT 2>&1; then
  echo "FAILED: java command returned error";
  echo "Check ${OUT}."
  exit 1
fi
tail $OUT -n 1
checkOutput
echo "It looks like the local execution went fine."

# 3. test cloud execution with file input

 OUT=$TEMP/cloud-file-out.txt
 CONTIGS=seq1:0:800
 rm -f $OUT
 gsutil rm "${OUTPUT}-000*" || echo ""
 echo "Testing cloud execution with a file (output in $OUT)."
 
 if ! java -XX:MaxPermSize=512m -Xms2g -Xmx3g -cp $JAR \
   com.google.cloud.genomics.dataflow.pipelines.CountReads \
   --project=$PROJECT_ID \
   --stagingLocation=${STAGING} \
   --numWorkers=2 \
   --runner=BlockingDataflowPipelineRunner \
   --output=$OUTPUT \
   --genomicsSecretsFile=$CLIENT_SECRETS \
   --references=$CONTIGS \
   --BAMFilePath=$GS_TEST_BAM > $OUT 2>&1; then
   echo "FAILED: java command returned error";
   echo "Check ${OUT}."
   exit 1
 fi
 tail $OUT -n 1
 checkOutput
 echo "It looks like the cloud execution went fine."

# 4. test cloud execution with API input

OUT=$TEMP/cloud-api-out.txt
CONTIGS=seq1:0:800
rm -f $OUT
gsutil rm "${OUTPUT}-000*" || echo ""
echo "Testing cloud execution with a file (output in $OUT)."

if ! java -XX:MaxPermSize=512m -Xms2g -Xmx3g -cp $JAR \
  com.google.cloud.genomics.dataflow.pipelines.CountReads \
  --project=$PROJECT_ID \
  --stagingLocation=${STAGING} \
  --numWorkers=2 \
  --runner=BlockingDataflowPipelineRunner \
  --output=$OUTPUT \
  --genomicsSecretsFile=$CLIENT_SECRETS \
  --references=$CONTIGS \
  --datasetId=$GS_TEST_DATASET --readGroupSetId=$GS_TEST_READGROUPSET > $OUT 2>&1; then
  echo "FAILED: java command returned error";
  echo "Check ${OUT}."
  exit 1
fi
tail $OUT -n 1
checkOutput
echo "It looks like the cloud execution went fine."
gsutil rm "${OUTPUT}-000*" || echo ""

echo "PASS: CountReads"
