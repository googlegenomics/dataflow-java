PROJECT=bioteam-dataflow-90121
bucket=gs://bioteamdataflow
OUTPUT=${bucket}/output_align.txt
STAGING=${bucket}/staging

# --project=${PROJECT}  --output=${OUTPUT}  --genomicsSecretsFile=client_secret.json
#java -cp target/google-genomics-dataflow-*.jar   com.google.cloud.genomics.dataflow.pipelines.AlignmentsReads  --help='com.google.cloud.genomics.dataflow.pipelines.AlignmentsReads$CountReadsOptions'

#--BAMFilePath=gs://bioteamdataflow/test.bam 
readgroupsetid=CMvnhpKTFhD04eLE-q2yxnU
#readgroupsetid=CNOc5OP9ERDzhNPG3_rwmZ4B
java -cp target/google-genomics-dataflow-*.jar   com.google.cloud.genomics.dataflow.pipelines.AlignmentsReads --project=${PROJECT}  --output=${OUTPUT}  --genomicsSecretsFile=client_secrets.json --shardBAMReading=false --readGroupSetId=${readgroupsetid}





