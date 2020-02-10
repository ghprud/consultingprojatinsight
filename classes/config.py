from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid
import constants

#TODO -> need more work on this. There should be just one config for the entire project
class Config:
    def __init__(self):
        self.spark = SparkSession.builder.appName(constants.Constants.get_app_name()).getOrCreate()
    
    def set_spark_config(self):
        self.spark.conf.set('temporaryGcsBucket', constants.Constants.get_gcs_bucket_name())
    
    def get_spark(self):
        return self.spark

config = Config()
config.set_spark_config()
