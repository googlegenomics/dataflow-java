PROJECT=bioteam-dataflow-90121
bucket=gs://bioteamdataflow
OUTPUT=${bucket}/output_align.txt
STAGING=${bucket}/staging

# --project=${PROJECT}  --output=${OUTPUT}  --genomicsSecretsFile=client_secret.json
#java -cp target/google-genomics-dataflow-*.jar   com.google.cloud.genomics.dataflow.pipelines.AlignmentsReads  --help='com.google.cloud.genomics.dataflow.pipelines.AlignmentsReads$CountReadsOptions'

#--BAMFilePath=gs://bioteamdataflow/test.bam 
#--readGroupSetId=CMvnhpKTFhD04eLE-q2yxnU
java -cp target/google-genomics-dataflow-*.jar   com.google.cloud.genomics.dataflow.pipelines.AlignmentsReads  --first100=true --project=${PROJECT}  --output=${OUTPUT}  --genomicsSecretsFile=client_secret.json --shardBAMReading=false --readGroupSetId=CMvnhpKTFhD04eLE-q2yxnU





