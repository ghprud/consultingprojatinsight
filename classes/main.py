from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

import constants
import firstsource
import secondsource
import utilities

class EmailAnalytics:

    def __init__(self):
        pass

    def aggregate_data(self):
        """
        Processes the data from different sources, joins, and saves to the data warehouse.
        Sets the spark config parameters that are passed to different methods.
        """
        spark = SparkSession.builder.master('yarn').appName(constants.Constants.get_app_name()).getOrCreate()

        spark.conf.set('temporaryGcsBucket', constants.Constants.get_gcs_bucket_name())
        '''
        spark.conf.set('spark.serialzier', 'org.apache.spark.serializer.KryoSerializer')
        spark.conf.set('spark.kryo.unsafe', 'false')
        spark.conf.set('spark.kryoserializer.buffer.max', '2048m')
        spark.conf.set('spark.kryo.registrationRequired', 'false')
        '''

        firstSource = firstsource.FirstSource()
        firstsource_df = firstSource.process_get_tilda_df_improved(spark)

        customer_df = secondsource.CustomerIOData()
        customer_io_df = customer_df.process_get_customerio_improved_df(spark)

        final_df = utilities.Utilities.merge_data_frames(firstsource_df, customer_io_df)
        utilities.Utilities.save_to_data_warehouse(final_df)

def main():
    emailAnalytics = EmailAnalytics()
    emailAnalytics.aggregate_data()

if __name__ == "__main__":
    main()