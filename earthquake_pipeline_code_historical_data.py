from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,expr
from util import Utils
from datetime import datetime
from google.cloud import bigquery


if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## initializtion of temp bucket for storing stagging data
    bucket = "earthquake_dataproc_temp_bucket"
    spark.conf.set("temporaryGcsBucket", bucket)

    ## API uri
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    # Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ##call class Utils
    util_obj = Utils()

    ## bigquery audit data store location
    audit_output_db = 'spark-learning-431506.earthquake_db.earthquake_audit_tbl'

    ## job_id for audit log
    job_id = cur_timestamp
    pipeline_name = "earthquake_pipeline_dev"

    ############################### function 1 : extractallData   ############################################################################################
    ## information collect for audit log regarding extract data function
    try:
        function_name = "1_extractallData"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call extractData function for extract all data from api
        all_data = util_obj.extractallData(api_url)

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

    ########################################### function 2: extractRequiredData #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "2_extractRequiredData"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call extractRequiredData function for fetch required data
        reuired_data_lst_of_dic = util_obj.extractRequiredData(all_data)

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

    ########################################### function 3: convertIntoDF #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "3_convertIntoDF"
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

    ########################################### function 4: writeIntoGcs #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "4_writeIntoGcs"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ## call function writeIntoGcs for write data into gsc bucket (earthquake_analysis)

        gcs_landing_location = f"gs://earthquake_analysis_buck/pyspark/landing/{cur_timestamp}"
        # gcs_landing_location = f"D:/Mohini Data Science/earthquake_ingestion/bronze/landing_data/earthquake{cur_timestamp}"
        util_obj.writeIntoGcs(earthquake_dataframe, gcs_landing_location)

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

#######################################################################################################################################################################


    ## read data from gcs
    gcs_input_location = gcs_landing_location


    ########################################### function 5: readDataFromloandingGCS #############################################################################################################

    ## information collect for audit log regarding extract data function
    try:
        function_name = "5_readDataFromloandingGCS"
        start_time = datetime.now().strftime('%Y%m%d_%H%M%S')

        ##call readDataFromloandingGCS function for read data
        earthquake_data = util_obj.readDataFromloandingGCS(spark, gcs_input_location)
        # earthquake_data.show()
        # earthquake_data.printSchema()

        end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        status = "successful"
        process_record = earthquake_data.count()

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
        flatten_data_df = util_obj.flattenData(earthquake_data)
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






















































