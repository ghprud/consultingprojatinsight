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
        Process the data from the second source

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
            .groupBy('data.XXXXX', 'XXXXX') \
            .count()

        # get the customer details #TODO find a better way to do it using the dataframe methods
        # need a unique set of email ids
        customer_data_details = spark.sql(
            '''
            select a.data.XXXX  AS XXXX,
            a.data.XXXX AS XXXX,
            a.data.variables.XXXx.type AS XXXX,
            a.event_type as XXXX,
            concat(a.data.XXXX, " || ", a.data.XXXXX ) AS XXXXX,
            a.XXX AS XXXX,
            a.data.variables.XXXX AS XXXX
            FROM XXX a
            '''
        )
        customer_data_details_filtered = customer_data_details \
            .where(customer_data_details.event_type == 'XXXX')

        joined_customer_details = customer_data_details_filtered.join (
            customer_email_event_filtered_temp, \
            customer_data_details_filtered.email_id == customer_email_event_filtered_temp.email_id, \
            how='inner'
        ).drop(customer_email_event_filtered_temp.email_id).drop(customer_data_details_filtered.event_type)

        pivoted_table = joined_customer_details \
            .groupBy('XX') \
            .pivot('event_type', \
                ['XXXXX', 'XXXX', 'XXXXX', 'XXXX', 'XXXXX', 'XXXXX']) \
            .sum('count').fillna(0)

        temp_final_customerio_table = pivoted_table.join(
            customer_data_details,
            customer_data_details.email_id == pivoted_table.email_id,
            how='inner'
        ).drop(pivoted_table.email_id)

        final_customerio_table = temp_final_customerio_table.select (
            col('XXXXX').alias('XXXXX'), \
            'XXXXX', \
            'XXXXX', \
            'XXXXX', \
            col('XXXX').cast(TimestampType()), \
            col('XXXX').alias('XXXX'), \
            col('XXXXXX').alias('XXXX'), \
            col('XXXX').alias('XXXXXX'), \
            col('XXXXX').alias('XXXX'), \
            col('XXXXX').alias('XXXX'), \
            col('XXXX').alias('XXXX'), \
            'XXXXXXXXXX',
        ).where(temp_final_customerio_table.event_type == 'XXXXXX') \
            .withColumn("data_source", lit("secondsource").cast(StringType())) \
            .dropDuplicates(['XXX', 'XXXX', 'XXXX'])
    

        return final_customerio_table