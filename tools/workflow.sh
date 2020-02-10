# omitting the variable values
# Your variables go here
export PROJECT_ID=XXXXX
export REGION=XXXX
export ZONE=XXXX
export BUCKET_NAME=gs://messaging

export CLUSTER_1=cluster-test-1

CLUSTER_UUID=$(gcloud dataproc clusters describe $CLUSTER_1 \
  --region $REGION \
  | grep 'goog-dataproc-cluster-uuid:' \
  | sed 's/.* //')

echo $CLUSTER_UUID

export TEMPLATE_ID=template-unified-messaging
gcloud dataproc workflow-templates create \
  $TEMPLATE_ID --region $REGION

#gcloud dataproc workflow-templates set-managed-cluster \
 # $TEMPLATE_ID \
  #--region $REGION \
  #--zone $ZONE \
  #--cluster-name single-node-cluster \
  #--single-node \
  #--master-machine-type n1-standard-1 \
  #--master-boot-disk-size 500 \
  #--image-version 1.3-deb9

#use the same cluster to run the email and sms aggregation job
gcloud dataproc workflow-templates set-cluster-selector \
  $TEMPLATE_ID \
  --region $REGION \
  --cluster-labels goog-dataproc-cluster-uuid=$CLUSTER_UUID

export STEP_ID=analytics
gcloud dataproc workflow-templates add-job pyspark \
  $BUCKET_NAME/main.py \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
  --step-id $STEP_ID \
  --workflow-template $TEMPLATE_ID \
  --region $REGION

gcloud dataproc workflow-templates instantiate \
  $TEMPLATE_ID --region $REGION --async