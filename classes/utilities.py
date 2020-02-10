from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
import uuid

#TODO: add validations
#TODO: add exeception handling
class Utilities:
    def __init__(self):
        pass

    def merge_data_frames(df1, df2):
        if ((df1 is not None) and (df2 is not None)):
            return df1.union(df2)
    
    def save_to_data_warehouse(final_df):
        """
        saves the final dataframe to the output table: email_analytics

        arguments:
        final_df -- the final dataframe that is saved to BigQuery
        """
        if (final_df is not None):
            final_df.write \
                .format('bigquery') \
                .option("table", Constants.get_output_table_name()) \
                .mode('overwrite') \
                .save()

    #omitting some values from the schema for public github
    def get_customer_schema( ):
        """
        customerSchema is needed to match the set of arguments from another table
        """
        customerSchema = [
            StructField("election_adjustment", LongType(), True),
            StructField("start_date", StringType(), True),
            StructField("mployer_id", LongType(), True),
            StructField("enrolled_in_healthcare_fsa", BooleanType(), True),
            StructField("address_city_name", StringType(), True),
            StructField("organization_id", LongType(), True),
            StructField("banking_institutions_connected_count", LongType(), True),
            StructField("hfsa_enabled", BooleanType(), True)
        ]
        return customerSchema