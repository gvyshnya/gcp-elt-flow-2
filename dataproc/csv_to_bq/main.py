from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.types import (StructType, StructField, StringType, 
    FloatType, DateType, ArrayType, DoubleType, BooleanType)

from datetime import datetime
import pandas as pd
import sys

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def load_csv_to_bigquery(
    csv_file: str,
    gcp_project_id: str,
    dataset_name: str,
    table_name: str,
    temp_gcp_bucket_name: str
):
    """
    Read the csv file from GCP,
    then uploads it to a target table in BigQuery.
    
    csv_file - either a fully qualified path or Google Cloud Storate uri of a csv file
    gcp_project_id - the google cloud project ID
    dataset_name - the name of the target dataset (database) in BigQuery
    table_name - the name of the target table in the BigQuery dataset 
                Note: if such a table exists, it will be overwritten with the new data from CSV file
    temp_gcp_bucket_name - the name of GCP bucket (wihtout uri prefix) used as a temp space for Spark to
                           write the data into BigQuery
    """
    
    # read the CSV file into memory
    df = spark.read.option("header", True) \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(csv_file)
    
    # Replaces dots with underscores in column names
    # It is required for smooth upload of data into BigQuery down the road
    df = df.toDF(*(c.replace('.', '_') for c in df.columns))
    
    # Saving the data to BigQuery
    # 'com.google.cloud.spark.bigquery'
    table = "{}.{}.{}".format(gcp_project_id, dataset_name, table_name)
    df.write.format('bigquery') \
        .option("temporaryGcsBucket", temp_gcp_bucket_name) \
        .option('table', table) \
        .mode("overwrite") \
        .save()

        
def main(argv):

    csv_file = argv[0]
    gcp_project_id = argv[1]
    dataset_name = argv[2]
    table_name = argv[3]
    temp_gcp_bucket_name = argv[4]
    
    load_csv_to_bigquery(
		csv_file,
		gcp_project_id, 
		dataset_name, 
		table_name,
		temp_gcp_bucket_name
	)


if __name__ == "__main__":
    main(sys.argv[1:])
    sc.stop()