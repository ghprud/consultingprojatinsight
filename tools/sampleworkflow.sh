#sample commands that can be used with workflow
#reference: https://github.com/garystafford/dataproc-workflow-templates
# good practice to keep the cloud sdk up-to-date
gcloud components update

# login and configure settings
gcloud init

# Your variables go here
export PROJECT_ID=dataproc-demo-224523
export REGION=us-east1
export ZONE=us-east1-b
export BUCKET_NAME=gs://dataproc-demo-bucket

gsutil cp data/ibrd-statement-of-loans-*.csv $BUCKET_NAME
gsutil cp build/libs/dataprocJavaDemo-1.0-SNAPSHOT.jar $BUCKET_NAME
gsutil cp international_loans_dataproc_large.py $BUCKET_NAME

export TEMPLATE_ID=template-demo-1
gcloud dataproc workflow-templates create \
  $TEMPLATE_ID --region $REGION

gcloud dataproc workflow-templates set-managed-cluster \
  $TEMPLATE_ID \
  --region $REGION \
  --zone $ZONE \
  --cluster-name single-node-cluster \
  --single-node \
  --master-machine-type n1-standard-1 \
  --master-boot-disk-size 500 \
  --image-version 1.3-deb9

gcloud dataproc workflow-templates set-managed-cluster \
  $TEMPLATE_ID \
  --region $REGION \
  --zone $ZONE \
  --cluster-name three-node-cluster \
  --master-machine-type n1-standard-4 \
  --master-boot-disk-size 500 \
  --worker-machine-type n1-standard-4 \
  --worker-boot-disk-size 500 \
  --num-workers 2 \
  --image-version 1.3-deb9

export STEP_ID=ibrd-large-pyspark
gcloud dataproc workflow-templates add-job pyspark \
  $BUCKET_NAME/international_loans_dataproc.py \
  --step-id $STEP_ID \
  --workflow-template $TEMPLATE_ID \
  --region $REGION \
  -- "gs://dataproc-demo-bucket" \
     "ibrd-statement-of-loans-historical-data.csv" \
     "ibrd-summary-large-python"

export STEP_ID=ibrd-small-spark
gcloud dataproc workflow-templates add-job spark \
  --region $REGION \
  --step-id $STEP_ID \
  --workflow-template $TEMPLATE_ID \
  --class org.example.dataproc.InternationalLoansAppDataproc \
  --jars $BUCKET_NAME/dataprocJavaDemo-1.0-SNAPSHOT.jar

export STEP_ID=ibrd-large-spark
gcloud dataproc workflow-templates add-job spark \
  --region $REGION \
  --step-id $STEP_ID \
  --workflow-template $TEMPLATE_ID \
  --class org.example.dataproc.InternationalLoansAppDataprocLarge \
  --jars $BUCKET_NAME/dataprocJavaDemo-1.0-SNAPSHOT.jar

yes | gcloud dataproc workflow-templates remove-job \
  $TEMPLATE_ID \
  --region $REGION \
  --step-id $STEP_ID

gcloud dataproc workflow-templates instantiate \
  $TEMPLATE_ID --region $REGION --async

https://github.com/google/re2/wiki/Syntax

gcloud dataproc workflow-templates import $TEMPLATE_ID \
   --region $REGION --source template-demo-param.yaml

time gcloud dataproc workflow-templates instantiate \
  $TEMPLATE_ID --region $REGION --async \
  --parameters MAIN_PYTHON_FILE="$BUCKET_NAME/international_loans_dataproc.py",STORAGE_BUCKET=$BUCKET_NAME,IBRD_DATA_FILE="ibrd-statement-of-loans-historical-data.csv",RESULTS_DIRECTORY="ibrd-summary-large-python"

time gcloud dataproc workflow-templates instantiate \
  $TEMPLATE_ID --region $REGION --async \
  --parameters MAIN_PYTHON_FILE="$BUCKET_NAME/international_loans_dataproc.py",STORAGE_BUCKET=$BUCKET_NAME,IBRD_DATA_FILE="ibrd-statement-of-loans-latest-available-snapshot.csv",RESULTS_DIRECTORY="ibrd-summary-small-python"

gcloud dataproc workflow-templates list --region $REGION

gcloud dataproc workflow-templates describe \
  $TEMPLATE_ID --region $REGION

gcloud dataproc operations list --region $REGION

gcloud dataproc operations describe \
  projects/$PROJECT_ID/regions/$REGION/operations/c1d89aa5-c276-3a55-b46d-30592c8e59fe

yes | gcloud dataproc operations cancel ea993765-edba-4db5-8536-a864227408d7

export TEMPLATE_ID=template-demo-1
yes | gcloud dataproc workflow-templates delete \
  $TEMPLATE_ID --region $REGION

export TEMPLATE_ID=template-demo-3
yes | gcloud dataproc workflow-templates delete \
  $TEMPLATE_ID --region $REGION

gcloud dataproc workflow-templates instantiate-from-file \
  --file template-demo.yaml \
  --region $REGION \
  --async

export TEMPLATE_ID=template-demo-4

gcloud dataproc workflow-templates create \
  $TEMPLATE_ID --region $REGION

gcloud dataproc workflow-templates set-cluster-selector \
  $TEMPLATE_ID \
  --region $REGION \
  --cluster-labels goog-dataproc-cluster-uuid=577ab78d-30a3-487c-8f5b-63a3e455b759

gcloud dataproc workflow-templates import $TEMPLATE_ID \
   --region $REGION --source template-demo-4.yaml

export SET_ID=ibrd-large-dataset-pyspark-cxzzhr2ro3i54

command=$(gcloud dataproc jobs wait $SET_ID \
  --project $PROJECT_ID \
  --region $REGION) &>/dev/null
if grep -Fqx "  state: FINISHED" <<< $command &>/dev/null;then echo "Success";else echo "Failure";fi

if grep -Fqx "  state: FINISHED" <<<$command &>/dev/null; then
  echo "Success"
else
  echo "Failure"
fi

