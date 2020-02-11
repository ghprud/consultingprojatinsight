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
        Process the data from two different data sources

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

        messaging_task_metrics_pivoted_table = table_messaging_task_metrics \
            .groupBy('XXX') \
            .pivot('XXX', \
                ['XXX', 'XX', 'XXXX', 'XXXX', 'XXX', 'XXX']) \
            .sum('metric_count').fillna(0)

        messaging_task_details = spark.sql(
            '''
            select mtasks.id as XXXX, mtasks.XXXXX as XXXXX, mtasks.XXXX as XXX,
            mtasks.XXXX as XXXXX, mtasks.XXXX as XXXX, 
            mtasks.XXXX as XXXX
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
            .withColumn("XXXXXX", lit(None).cast(StructType(Utilities.get_customer_schema()))) \
            .withColumn("XXXXX", lit('firstsource').cast(StringType()))

        final_msg_table = temp_final_msg_table.select( \
            col('XXXXX').alias('XXXXXXX'), \
            'XXXXX', \
            'XX', \
            'XXXXXXXX', \
            col('XXXXX').cast(TimestampType()), \
            'XXXXX', \
            'XXXX', \
            'XXXXX', \
            'XXXXX', \
            'XX', \
            'XX', \
            'XXXX',
            'XXXX'
        )
        return final_msg_table
