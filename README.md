# consultingprojatinsight

Technologies/Programming Languages used:
    1. Python
    2. Spark
    3. Google Cloud Platform

Project setup:
    1. Set up a cluster 
        a. The cluster can be set up through the DataProc console on GCP (The cluster that runs the pyspark jobs
            on GCP are a four node cluster: one master and three worker nodes)
        b. The cluster can be set up via gcloud commands as well. The sampleworkflow.sh in the tools folder
            can be used as a reference to set up a cluster via gcloud commands. 
            (Note: using gcloud on an instance will require having the right set of permissions for the service account)
    2. Set up a workflow template to run the job
        a. The shell script (workflow.sh) in the tools folder sets up a workflow template - 
        b. There is a workflow template that is already set up. You can click "RUN" at (omitted for public github)to run the job. 
    3. Behind the scenes:
        a. The python scripts that does the reading, and transformation of the data is copied to the storage bucket
        b. The PySpark job is set up to run the script from the above mentioned bucket
        c. The final output of the script is saved to the table on BigQuery
    4. How can the whole process be automated? (work in progress)
        a. Run a cron job from an compute engine instance. 
            i. Check out the code from github, and run the job.sh script from the tools folder. 
                This job instantiates the workflow template that is already set up. 
            i. blocker: the service account do not have the right credentials.
        b. Set up a "serverless" mode that can use GCP's Cloud Scheduler, and Pub Sub to run the batch job 
            i. AWS has AWS Batch to run batch jobs. GCP doesn't have something similar. 
            ii. Instructions to set up the batch flow on GCP - 
                https://medium.com/google-cloud/running-a-serverless-batch-workload-on-gcp-with-cloud-scheduler-cloud-functions-and-compute-86c2bd573f25

Testing:
    1. The job reads the data from three different sources and puts the final set of data into unified_messaging_dev.email_analytics table.
    2. There should be one row per email_id. Hence, the following query will yeild zero results:
        select a.original_email_id, count(*)  from `table_name` a
        group by a.original_email_id having count(*) > 1;
    



