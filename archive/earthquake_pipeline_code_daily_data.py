from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,expr
from utils import Utils
from datetime import datetime
from google.cloud import bigquery
import argparse

if __name__ == '__main__':
    ## Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## initializtion of temp bucket for storing stagging data
    bucket = "earthquake_dataproc_temp_bucket"
    spark.conf.set("temporaryGcsBucket", bucket)

    ## API uri
    ##monthly
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    ## daily
    # api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    ## Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ##call class Utils
    util_obj = Utils()

    ## bigquery audit data store location
    audit_output_db = 'spark-learning-431506.earthquake_db.earthquake_audit_tbl'

############################### function 1 : extractallData   ############################################################################################

    # Call a function to extract all data from the API (the function extractallData should return the data as a string)
    source_data = util_obj.extractallData(api_url)

########################################### function 2: writeExtractDataintoGCS #############################################################################################################

    ## call function writeExtractDataintoGCS for write data in bucket
    # Initialize the GCP project ID
    project_id = 'spark-learning-431506'
    # Define the GCS bucket name where the data will be stored
    load_data_bucket_name = 'earthquake_analysis_buck'
    # Set the destination blob (file) name with a unique timestamp
    destination_blob_name = f'pyspark/landing/{cur_timestamp}'
    ## call function writeExtractDataintoGCS
    util_obj.writeExtractDataintoGCS(project_id, source_data, load_data_bucket_name, destination_blob_name, api_url)

########################################### function 3: readDataFromLandingGcs #############################################################################################################

    ## read data(json) from gcs bucket(from landing or bronze layer) by using readDataFromLandingGcs function
    # Specify the GCS bucket and blob name
    read_data_bucket_name = load_data_bucket_name
    read_data_location = destination_blob_name
    # call function readDataFromLandingGcs
    json_data = util_obj.readDataFromLandingGcs(project_id,read_data_bucket_name, read_data_location)
    # print(json_data,type(json_data)) ##dict

########################################### function 4: extractRequiredData #############################################################################################################

    ## call extractRequiredData function for fetch required data
    reuired_data_lst_of_dic = util_obj.extractRequiredData(json_data)

########################################### function 5: convertIntoDF #############################################################################################################

    ## call convertIntoDF function for convert into dataframe
    earthquake_dataframe = util_obj.convertIntoDF(spark, reuired_data_lst_of_dic)
    # earthquake_dataframe.show()

########################################### function 6: flattenData #############################################################################################################

    ## call flattenData function for flattening the data
    flatten_data_df = util_obj.flattenData(earthquake_dataframe)
    flatten_data_df.show(truncate=False)
    # flatten_data_df.printSchema()
########################################### function 7: writeIntoGcs #############################################################################################################

    ## upload flatten data in silver layer using  writeIntoGcs function from utils
    output_path = f"gs://earthquake_analysis_buck/pyspark/silver/{cur_timestamp}"
    # output_path = f"D:/Mohini Data Science/earthquake_ingestion/silver/intermediate_data/{cur_timestamp}"
    util_obj.writeIntoGcs(flatten_data_df, output_path)
########################################### function 8: writeDataBigquery #############################################################################################################
    ## write data in bigquery
    output_db = 'spark-learning-431506.earthquake_db.earthquake_data'
    util_obj.writeDataBigquery(output_db, flatten_data_df)























































