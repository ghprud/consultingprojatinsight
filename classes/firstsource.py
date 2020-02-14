from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

import constants

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
        table1 = spark.read.format('bigquery') \
        .option('table', Constants.get_table1()) \
            .option("viewsEnabled", "true") \
                .load()
        table1.createOrReplaceTempView('table1')

        table2 = spark.read.format('bigquery') \
        .option('table', constants.Constants.get_table2()) \
            .option("viewsEnabled", "true") \
                .load()
        table2.createOrReplaceTempView('table2')

        table2_pivoted_table = table2 \
            .groupBy('XXX') \
            .pivot('XXX', \
                ['XXX', 'XX', 'XXXX', 'XXXX', 'XXX', 'XXX']) \
            .sum('metric_count').fillna(0)

        table1_details = spark.sql(
            '''
            select mtasks.id as XXXX, mtasks.XXXXX as XXXXX, mtasks.XXXX as XXX,
            mtasks.XXXX as XXXXX, mtasks.XXXX as XXXX, 
            mtasks.XXXX as XXXX
            from table1 mtasks
            '''
        )
        table1_details_filtered = table1_details \
            .filter((col('sent_at').isNotNull()) & (col('medium') == 'email'))

        joined_table2_table2 = table1_details.join (
            table2_pivoted_table, \
            table1_details.email_id == table2_pivoted_table.task_id,
            how='inner'
        )

        temp_final_msg_table = joined_table2_table2 \
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
