from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,expr
from util import Utils
from datetime import datetime
from google.cloud import bigquery
import argparse

if __name__ == '__main__':
    ## Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## initializtion of temp bucket for storing stagging data
    bucket = "earthquake_dataproc_temp_bucket"
    spark.conf.set("temporaryGcsBucket", bucket)

    ## Create an argument parser to handle command-line arguments
    parser = argparse.ArgumentParser()
    ## Add a required argument for the API URL with a help description
    parser.add_argument('-api_url', '--api_url', required=True, help='API URL required')
    parser.add_argument('-pipeline_nm', '--pipeline_nm', required=True, help='pipeline name ')

    ## Parse the command-line arguments
    arg = parser.parse_args()
    ## Assign the parsed API URL to a variable
    api_url = arg.api_url
    pipeline_name=arg.pipeline_nm

    ## API uri
    ##monthly
    # api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    ## daily
    # api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    ## Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ##call class Utils
    util_obj = Utils()

    ## bigquery audit data store location
    audit_output_db = 'spark-learning-431506.earthquake_db.earthquake_audit_tbl'

    ## job_id for audit log
    job_id = cur_timestamp
    # pipeline_name='daily'


    ############################### function 1 : extractallData   ############################################################################################
    ## information collect for audit log regarding extract data function
    try:
        function_name = "1_extractallData"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Call a function to extract all data from the API (the function extractallData should return the data as a string)
        source_data = util_obj.extractallData(api_url)

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = 0

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)


    ########################################### function 2: writeExtractDataintoGCS #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "2_writeExtractDataintoGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call function writeExtractDataintoGCS for write data in bucket
        # Initialize the GCP project ID
        project_id = 'spark-learning-431506'
        # Define the GCS bucket name where the data will be stored
        load_data_bucket_name = 'earthquake_analysis_buck'
        # Set the destination blob (file) name with a unique timestamp
        destination_blob_name = f'pyspark/landing/{cur_timestamp}'
        ## call function writeExtractDataintoGCS
        util_obj.writeExtractDataintoGCS(project_id, source_data, load_data_bucket_name, destination_blob_name, api_url)

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = 0

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)

    ########################################### function 3: readDataFromLandingGcs #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "3_readDataFromLandingGcs"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## read data(json) from gcs bucket(from landing or bronze layer) by using readDataFromLandingGcs function
        # Specify the GCS bucket and blob name
        read_data_bucket_name = load_data_bucket_name
        read_data_location = destination_blob_name
        # call function readDataFromLandingGcs
        json_data = util_obj.readDataFromLandingGcs(project_id,read_data_bucket_name, read_data_location)
        # print(json_data,type(json_data)) ##dict

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = 0

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)

    ########################################### function 4: extractRequiredData #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "4_extractRequiredData"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call extractRequiredData function for fetch required data
        reuired_data_lst_of_dic = util_obj.extractRequiredData(json_data)

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = 0

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)

    ########################################### function 5: convertIntoDF #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "5_convertIntoDF"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call convertIntoDF function for convert into dataframe
        earthquake_dataframe = util_obj.convertIntoDF(spark, reuired_data_lst_of_dic)
        # earthquake_dataframe.show()

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = earthquake_dataframe.count()

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)


########################################### function 6: flattenData #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "6_flattenData"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call flattenData function for flattening the data
        flatten_data_df = util_obj.flattenData(earthquake_dataframe)
        flatten_data_df.show(truncate=False)
        # flatten_data_df.printSchema()

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = flatten_data_df.count()

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)



    ########################################### function 7: writeIntoGcs #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "7_writeIntoGcs"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## upload flatten data in silver layer using  writeIntoGcs function from utils
        output_path = f"gs://earthquake_analysis_buck/pyspark/silver/{cur_timestamp}"
        # output_path = f"D:/Mohini Data Science/earthquake_ingestion/silver/intermediate_data/{cur_timestamp}"
        util_obj.writeIntoGcs(flatten_data_df, output_path)

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = flatten_data_df.count()

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)



    ########################################### function 8: writeDataBigquery #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "8_writeDataBigquery"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## write data in bigquery
        output_db = 'spark-learning-431506.earthquake_db.earthquake_data'
        util_obj.writeDataBigquery(output_db, flatten_data_df)

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = flatten_data_df.count()

    except:
        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "fail"
        process_record = 0

    ## create audit data fram by using  createDFforAuditTbl function
    audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)

    ## call auditTblSchema function for get audit table schema
    audit_table_schema = util_obj.auditTblSchema()

    ## write audit data to bigquery by using writeDataBigquery function
    util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)






















































