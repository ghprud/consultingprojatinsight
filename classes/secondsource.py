from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

import constants

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
        table1 = spark.read.format('bigquery') \
            .option('table', constants.Constants.get_customer_io_table()) \
            .option("viewsEnabled", "true") \
            .load().cache()
        table1.createOrReplaceTempView('table1')

        table1_filtered = table1 \
            .filter(table1.event_type != "XXl_XXt")
            
        table1_filtered_temp = table1_filtered \
            .groupBy('data.XXXXX', 'XXXXX') \
            .count()

        # get the customer details #TODO find a better way to do it using the dataframe methods
        # need a unique set of email ids
        table1_details = spark.sql(
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
        table1_details_filtered = table1_details \
            .where(table1_details.event_type == 'XXXX')

        joined_table1_details = table1_details_filtered.join (
            table1_filtered_temp, \
            table1_details_filtered.XXX_id == table1_filtered_temp.XXX_id, \
            how='inner'
        ).drop(table1_details_filtered.XXX_id).drop(table1_details_filtered.XXXX_type)

        pivoted_table = joined_table1_details \
            .groupBy('XX') \
            .pivot('XXX_type', \
                ['XXXXX', 'XXXX', 'XXXXX', 'XXXX', 'XXXXX', 'XXXXX']) \
            .sum('count').fillna(0)

        temp_final_table = pivoted_table.join(
            customer_data_details,
            customer_data_details.XXX_id == pivoted_table.XXX_id,
            how='inner'
        ).drop(pivoted_table.XXX_id)

        final_table = temp_final_table.select (
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
        ).where(temp_final_table.event_type == 'XXXXXX') \
            .withColumn("data_source", lit("secondsource").cast(StringType())) \
            .dropDuplicates(['XXX', 'XXXX', 'XXXX'])
    

        return final_table