from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

#the constant values have been changed.
class Constants:
    @staticmethod
    def get_app_name( ):
        return 'messaging'

    @staticmethod
    def get_output_table_name( ):
        return 'analytics_output'

    @staticmethod
    def get_gcs_bucket_name( ):
        return 'messaging-bucket'

    @staticmethod
    def get_table1( ):
        return 'msg_tasks'
    
    @staticmethod
    def get_table2( ):
        return 'xxxx'

    @staticmethod
    def get_table3( ):
        return 'cust_data'
    
    @staticmethod
    def get_unique_ids_table():
        return 'uniqueids'