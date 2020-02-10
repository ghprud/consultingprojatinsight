from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

class FirstSource:

    def __init__(self):
        pass

    #TODO: The data transformation and aggregation process can be optimized
    #TODO: add proper validations
    #TODO: need better variable names
    def process_get_first_source_df_improved(self, spark):
        """
        Process the data from messaging_tasks and messaging_task_metrics tables.

        arguments:
        spark -- variable with the spark config parameters

        retunrs the dataframe with the required selected values
        """
        table_messaging_tasks = spark.read.format('bigquery') \
        .option('table', Constants.get_messaging_tasks_table_name()) \
            .option("viewsEnabled", "true") \
                .load()
        table_messaging_tasks.createOrReplaceTempView('table_messaging_tasks')

        table_messaging_task_metrics = spark.read.format('bigquery') \
        .option('table', Constants.get_messaging_task_metrics_table_name()) \
            .option("viewsEnabled", "true") \
                .load()
        table_messaging_task_metrics.createOrReplaceTempView('table_messaging_task_metrics')

        '''
            bounced, delivered, unsubscribed, spamreported, opened, clicked
        '''
        messaging_task_metrics_pivoted_table = table_messaging_task_metrics \
            .groupBy('task_id') \
            .pivot('metric_name', \
                ['bounced', 'opened', 'delivered', 'unsubscribed', 'spamreported', 'clicked']) \
            .sum('metric_count').fillna(0)

        messaging_task_details = spark.sql(
            '''
            select mtasks.id as email_id, mtasks.employee_id as employee_id, mtasks.medium as medium,
            mtasks.message_type as campaign_type, mtasks.message_subtype as campaign_name, 
            mtasks.sent_at as sent_at
            from table_messaging_tasks mtasks
            '''
        )
        messaging_task_details_filtered = messaging_task_details \
            .filter((col('sent_at').isNotNull()) & (col('medium') == 'email'))

        joined_messaging_tasks_metrics = messaging_task_details.join (
            messaging_task_metrics_pivoted_table, \
            messaging_task_details.email_id == messaging_task_metrics_pivoted_table.task_id,
            how='inner'
        )

        temp_final_msg_table = joined_messaging_tasks_metrics \
            .withColumn("customer_attributes", lit(None).cast(StructType(Utilities.get_customer_schema()))) \
            .withColumn("data_source", lit('firstsource').cast(StringType()))

        final_msg_table = temp_final_msg_table.select( \
            col('email_id').alias('original_email_id'), \
            'employee_id', \
            'campaign_type', \
            'campaign_name', \
            col('sent_at').cast(TimestampType()), \
            'bounced', \
            'opened', \
            'delivered', \
            'unsubscribed', \
            'spamreported', \
            'clicked', \
            'customer_attributes',
            'data_source'
        )
        return final_msg_table
