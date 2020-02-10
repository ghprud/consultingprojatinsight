from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

class Customer:

    def __init__(self):
        pass

    #TODO: The data transformation and aggregation process can be optimized
    #TODO: The data transformation and aggregation process can be optimized
    #TODO: add proper validations
    #TODO: need better variable names
    def process_get_customerio_improved_df(self, spark):
        """
        Process the data from 'alice-bip.stitch_customerio.data_latest_view'

        arguments:
        spark -- variable with the spark config parameters

        retunrs the dataframe with the required selected values
        """
        tablecustomer = spark.read.format('bigquery') \
            .option('table', Constants.get_customer_io_table()) \
            .option("viewsEnabled", "true") \
            .load().cache()
        tablecustomer.createOrReplaceTempView('tablecustomer')

        customer_email_event_filtered = tablecustomer \
            .filter(tablecustomer.event_type != "email_sent")
            
        customer_email_event_filtered_temp = customer_email_event_filtered \
            .groupBy('data.email_id', 'event_type') \
            .count()

        # get the customer details #TODO find a better way to do it using the dataframe methods
        # need a unique set of email ids
        customer_data_details = spark.sql(
            '''
            select a.data.email_id  AS email_id,
            a.data.customer_id AS employee_id,
            a.data.variables.campaign.type AS campaign_type,
            a.event_type as event_type,
            concat(a.data.campaign_name, " || ", a.data.message_name ) AS campaign_name,
            a.timestamp AS sent_at,
            a.data.variables.customer AS customer_attributes
            FROM tablecustomer a
            '''
        )
        customer_data_details_filtered = customer_data_details \
            .where(customer_data_details.event_type == 'email_sent')

        joined_customer_details = customer_data_details_filtered.join (
            customer_email_event_filtered_temp, \
            customer_data_details_filtered.email_id == customer_email_event_filtered_temp.email_id, \
            how='inner'
        ).drop(customer_email_event_filtered_temp.email_id).drop(customer_data_details_filtered.event_type)

        pivoted_table = joined_customer_details \
            .groupBy('email_id') \
            .pivot('event_type', \
                ['email_bounced', 'email_opened', 'email_delivered', 'email_unsubscribed', 'email_spamreported', 'email_clicked']) \
            .sum('count').fillna(0)

        temp_final_customerio_table = pivoted_table.join(
            customer_data_details,
            customer_data_details.email_id == pivoted_table.email_id,
            how='inner'
        ).drop(pivoted_table.email_id)

        final_customerio_table = temp_final_customerio_table.select (
            col('email_id').alias('original_email_id'), \
            'employee_id', \
            'campaign_type', \
            'campaign_name', \
            col('sent_at').cast(TimestampType()), \
            col('email_bounced').alias('bounced'), \
            col('email_opened').alias('opened'), \
            col('email_delivered').alias('delivered'), \
            col('email_unsubscribed').alias('unsubscribed'), \
            col('email_spamreported').alias('spamreported'), \
            col('email_clicked').alias('clicked'), \
            'customer_attributes',
        ).where(temp_final_customerio_table.event_type == 'email_sent') \
            .withColumn("data_source", lit("secondsource").cast(StringType())) \
            .dropDuplicates(['original_email_id', 'employee_id', 'sent_at'])
    

        return final_customerio_table