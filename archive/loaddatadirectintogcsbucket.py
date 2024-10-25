from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,substring,instr,length,expr
from pyspark.sql import Row
import requests
from datetime import datetime
from google.cloud import bigquery,storage
import os
import json
from util import Utils

## define extractallData function
def extractallData(api_url):
    ## by using get method extract the data from api
    response = requests.get(api_url)

    ##Check if the request was successful
    if response.status_code == 200:
        ##convert data into json
        all_data = response.json() #converts the (api)JSON response data into Python data types (usually a dictionary or a list).
        # print("Extracted Data:", data)
        print(f"extract data successfully from {api_url}")
        return json.dumps(all_data)  # Convert the Python dictionary to a JSON string

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

def writeExtractDataintoGCS(project_id,source_data,bucket_name,destination_blob_name,api_url):
    # Create a GCS client using the specified project ID
    client = storage.Client(project=project_id)

    # Get the GCS bucket object
    bucket_obj = client.bucket(bucket_name)

    # Create a new blob (file) in the bucket with the specified name
    blob = bucket_obj.blob(destination_blob_name)


    # Upload the extracted data to the GCS bucket
    blob.upload_from_string(
        data=source_data,
        content_type='application/json',timeout=100
    )
    print(f" write data successfully in {bucket_name}/{destination_blob_name}")

## define function readDataFromLandingGcs for read data from gcs bucket(from landing or bronze layer)

def readDataFromLandingGcs(project_id,bucket_name,read_data_location):
    # Initialize the Google Cloud Storage client
    client = storage.Client(project=project_id)

    # Get the bucket object
    bucket = client.bucket(bucket_name)

    # Get the blob (file) from the bucket
    blob = bucket.blob(read_data_location)

    # Download the blob's content as a string
    data_string = blob.download_as_string()

    # Convert the string to JSON (Python dictionary)
    data_json = json.loads(data_string)
    print(f"read data successfully from {bucket_name}/{read_data_location}")
    return data_json




######

## define extractRequiredData function
def extractRequiredData(data):
    ## fetch the metadata
    metadata_dic = data["metadata"]

    ## fetch count of records
    cnt_rcd = metadata_dic['count']
    print(f"total number of records {cnt_rcd}")

    ## fetch the required data (features)
    required_data = data['features']
    # print(required_data,type(required_data)) ##list

    reuired_data_lst = []
    for dict in required_data:
        ## fetch properties
        properties_dic = dict["properties"]

        ## add geometry cordinate in  properties
        properties_dic["geometry"] = dict["geometry"]["coordinates"]

        ## append properties_dic in list
        reuired_data_lst.append(properties_dic)

    print(reuired_data_lst)
    return reuired_data_lst

def dfSchema():
    ### Define the schema

    DF_schema = StructType([
        StructField("mag", StringType(), True),
        StructField("place", StringType(), True),
        StructField("time", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("tz", StringType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", StringType(), True),
        StructField("cdi", StringType(), True),
        StructField("mmi", StringType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", StringType(), True),
        StructField("rms", StringType(), True),
        StructField("gap", StringType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("geometry", ArrayType(StringType()), True)


    ])
    return  DF_schema

# ## define convertIntoDF function
# def convertIntoDF(self,spark,reuired_data_lst_of_dict,data_frame_schema = None):
#     if data_frame_schema is None:
#         ## call function dfSchema to get a schema
#         data_frame_schema = self.dfSchema()
#     ## convert into df
#     earthquake_data = spark.createDataFrame(reuired_data_lst_of_dict, schema=data_frame_schema)
#     # earthquake_data.show()
#     # earthquake_data.printSchema()
#     return earthquake_data






if __name__ == '__main__':
    import os
    from google.cloud import storage
    from datetime import datetime

    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## initializtion of temp bucket for storing stagging data
    bucket = "earthquake_dataproc_temp_bucket"
    spark.conf.set("temporaryGcsBucket", bucket)
    ##call class Utils
    util_obj = Utils()

    # Set the path to your Google Cloud service account key
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\GCP\Python_gcp\spark-learning-431506-160e4ffdff74_key.json"

    # Get the current date and time in 'YYYYMMDD_HHMMSS' format, which will be used to create a unique destination path
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Define the API URL for fetching earthquake data
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    # Initialize the GCP project ID
    project_id = 'spark-learning-431506'

    # Define the GCS bucket name where the data will be stored
    load_data_bucket_name = 'earthquake_analysis_buck'

    # Set the destination blob (file) name with a unique timestamp
    destination_blob_name = f'pyspark/landing/{cur_timestamp}'

    # Call a function to extract all data from the API (the function extractallData should return the data as a string)
    source_data = extractallData(api_url)


    ## call function writeExtractDataintoGCS for write data in bucket
    writeExtractDataintoGCS(project_id,source_data, load_data_bucket_name, destination_blob_name, api_url)

    ## read data(json) from gcs bucket(from landing or bronze layer) by using readDataFromLandingGcs function
    # Specify the GCS bucket and blob name
    read_data_bucket_name = load_data_bucket_name
    read_data_location = destination_blob_name
    # call function readDataFromLandingGcs
    json_data = readDataFromLandingGcs(project_id,read_data_bucket_name,read_data_location)
    # print(json_data,type(json_data)) ##dict

    ## call extractRequiredData function for fetch required data
    reuired_data_lst_of_dic = extractRequiredData(json_data)

    ## call convertIntoDF function for convert into dataframe
    earthquake_dataframe = util_obj.convertIntoDF(spark, reuired_data_lst_of_dic)
    earthquake_dataframe.show()



#############################################################################################################################################################3


# from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
# from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,substring,instr,length,expr
# from pyspark.sql import Row
# import requests
# from datetime import datetime
# from google.cloud import bigquery
#
# class Utils():
#     ## define extractallData function
#     def extractallData(self,api_url):
#         ## by using get method extract the data from api
#         response = requests.get(api_url)
#
#         ##Check if the request was successful
#         if response.status_code == 200:
#             ##convert data into json
#             all_data = response.json()
#             # print("Extracted Data:", data)
#             return all_data
#         else:
#             print(f"Failed to retrieve data. Status code: {response.status_code}")
#
#     ## define extractRequiredData function
#     def extractRequiredData(self,data):
#         ## fetch the metadata
#         metadata_dic = data["metadata"]
#
#         ## fetch count of records
#         cnt_rcd = metadata_dic['count']
#         print(f"total number of records {cnt_rcd}")
#
#         ## fetch the required data (features)
#         required_data = data['features']
#         # print(required_data,type(required_data)) ##list
#
#         reuired_data_lst = []
#         for dict in required_data:
#             ## fetch properties
#             properties_dic = dict["properties"]
#
#             ## add geometry cordinate in  properties
#             properties_dic["geometry"] = dict["geometry"]["coordinates"]
#
#             ## append properties_dic in list
#             reuired_data_lst.append(properties_dic)
#
#         # print(reuired_data_lst_of_dic)
#         return reuired_data_lst
#
#     def dfSchema(self):
#         ### Define the schema
#
#         DF_schema = StructType([
#             StructField("mag", StringType(), True),
#             StructField("place", StringType(), True),
#             StructField("time", StringType(), True),
#             StructField("updated", StringType(), True),
#             StructField("tz", StringType(), True),
#             StructField("url", StringType(), True),
#             StructField("detail", StringType(), True),
#             StructField("felt", StringType(), True),
#             StructField("cdi", StringType(), True),
#             StructField("mmi", StringType(), True),
#             StructField("alert", StringType(), True),
#             StructField("status", StringType(), True),
#             StructField("tsunami", IntegerType(), True),
#             StructField("sig", IntegerType(), True),
#             StructField("net", StringType(), True),
#             StructField("code", StringType(), True),
#             StructField("ids", StringType(), True),
#             StructField("sources", StringType(), True),
#             StructField("types", StringType(), True),
#             StructField("nst", IntegerType(), True),
#             StructField("dmin", StringType(), True),
#             StructField("rms", StringType(), True),
#             StructField("gap", StringType(), True),
#             StructField("magType", StringType(), True),
#             StructField("type", StringType(), True),
#             StructField("title", StringType(), True),
#             StructField("geometry", ArrayType(StringType()), True)
#
#
#         ])
#         return  DF_schema
#
#     ## define convertIntoDF function
#     def convertIntoDF(self,spark,reuired_data_lst_of_dict,data_frame_schema = None):
#         if data_frame_schema is None:
#             ## call function dfSchema to get a schema
#             data_frame_schema = self.dfSchema()
#         ## convert into df
#         earthquake_data = spark.createDataFrame(reuired_data_lst_of_dict, schema=data_frame_schema)
#         # earthquake_data.show()
#         # earthquake_data.printSchema()
#         return earthquake_data
#
#     ## define writeIntoGcs function for write data in gcs bucket
#     def writeIntoGcs(self, earthquake_df, output_path):
#
#         # Attempt to write the DataFrame to JSON
#         earthquake_df.coalesce(2).write.mode('overwrite').json(output_path)
#         print(f"data write successfully in {output_path}")
#
#
#
#     ## define function readDataFromloandingGCS for read data from gcs bucket
#     def readDataFromloandingGCS(self,spark, input_path):
#         ## call function dfSchema to get a schema
#         data_frame_schema = self.dfSchema()
#
#         # Attempt to read the JSON file
#         earthquake_df = spark.read.json(input_path, schema=data_frame_schema)
#         print("read data successfully.")
#         return earthquake_df
#
#
#     def flattenData(self,earthquake_df):
#         ## flatten the data
#         ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
#         ## Using split() to extract area and Generate column “area” -
#         ## add one cloumn insert date
#
#         flatten_data_df = (earthquake_df
#                            .withColumn('time', to_timestamp(from_unixtime(col('time') / 1000)))
#                            .withColumn('updated', to_timestamp(from_unixtime(col('updated') / 1000)))
#                            .withColumn('area', expr("substring(place, instr(place, 'of') + 3, length(place))"))
#                            .withColumn('longtitude', col('geometry').getItem(0).cast('float'))
#                            .withColumn('latitude', col('geometry').getItem(1).cast('float'))
#                            .withColumn('depth', col('geometry').getItem(2).cast('float'))
#                            .withColumn('insert_date',
#                                        current_timestamp())  ##  we can also use  lit(insert_date )= datetime.now().strftime('%Y%m%d %H%M%S')
#                            .drop(col("geometry"))
#
#                            )
#
#         return flatten_data_df
#
#         ## schema for bigquery
#     def bqSchema(self):
#     ### Define the schema
#
#         bq_schema = """
#         mag:STRING,
#         place:STRING,
#         time:TIMESTAMP,
#         updated:TIMESTAMP,
#         tz:STRING,
#         url:STRING,
#         detail:STRING,
#         felt:STRING,
#         cdi:STRING,
#         mmi:STRING,
#         alert:STRING,
#         status:STRING,
#         tsunami:INTEGER,
#         sig:INTEGER,
#         net:STRING,
#         code:STRING,
#         ids:STRING,
#         sources:STRING,
#         types:STRING,
#         nst:INTEGER,
#         dmin:STRING,
#         rms:STRING,
#         gap:STRING,
#         magType:STRING,
#         type:STRING,
#         title:STRING,
#         area:STRING,
#         longtitude:FLOAT,
#         latitude:FLOAT,
#         depth:FLOAT,
#         insert_date:TIMESTAMP
#         """
#
#         return bq_schema
#
#
#     ## define function for write data in bigquery
#     def writeDataBigquery(self,output_db, data_df,bq_schema=None):
#         if bq_schema is None:
#             ##call function bqSchema to get bq schema
#             bq_schema = self.bqSchema()
#
#         print(f'{data_df.count()}: no of records ')
#
#         data_df.write.format('bigquery').option("table", output_db) \
#             .option("schema", bq_schema) \
#             .option("createDisposition", "CREATE_IF_NEEDED") \
#             .option("writeDisposition", "WRITE_APPEND") \
#             .mode('append') \
#             .save()
#         print(f"load data successfully in {output_db}")
#
#
#     ## define createDFforAuditTbl function for create df for audit data
#     def createDFforAuditTbl(self,spark_1, job_id, pipeline_name, function_name, start_time, end_time, status,
#                             process_record=0):
#         audit_entry = [Row(job_id=job_id,
#                            pipeline_name=pipeline_name,
#                            function_name=function_name,
#                            start_time=start_time,
#                            end_time=end_time,
#                            status=status,
#                            process_record=process_record)]
#
#         schema = StructType([
#             StructField('job_id', StringType(), True),
#             StructField('pipeline_name', StringType(), True),
#             StructField('function_name', StringType(), True),
#             StructField('start_time', StringType(), True),
#             StructField('end_time', StringType(), True),
#             StructField('status', StringType(), True),
#             StructField('process_record', IntegerType(), True),
#
#         ])
#
#         # Create DataFrame with the provided schema
#         audit_df = spark_1.createDataFrame(audit_entry, schema)
#
#         # Show the DataFrame
#         # audit_df.show(truncate=False)
#         # audit_df.printSchema()
#         return audit_df
#
#     ### defind function for create audit tbl schema
#     def auditTblSchema(self):
#         audit_table_schema  = """
#             job_id:STRING,
#             pipeline_name:STRING,
#             function_name:STRING,
#             start_time:STRING,
#             end_time:STRING,
#             status:STRING,
#             process_record:INTEGER
#         """
# #         return audit_table_schema
# #
# #
# #
# #
# #
# #
# ############################################################################################
#
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType
# from pyspark.sql.functions import col, from_unixtime, split, trim, lit, to_timestamp, current_timestamp, expr
# from util import Utils
# from datetime import datetime
# from google.cloud import bigquery
#
# if __name__ == '__main__':
#     # Initialize Spark session
#     spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()
#
#     ## initializtion of temp bucket for storing stagging data
#     bucket = "earthquake_dataproc_temp_bucket"
#     spark.conf.set("temporaryGcsBucket", bucket)
#
#     ## API uri
#     api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
#
#     # Get the current date and time in 'YYYYMMDD_HHMMSS' format
#     cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#     ##call class Utils
#     util_obj = Utils()
#
#     ## bigquery audit data store location
#     audit_output_db = 'spark-learning-431506.earthquake_db.earthquake_audit_tbl'
#
#     ## job_id for audit log
#     job_id = cur_timestamp
#     pipeline_name = "earthquake_pipeline_dev"
#
#     ############################### function 1 : extractallData   ############################################################################################
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "1_extractallData"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         # Call a function to extract all data from the API (the function extractallData should return the data as a string)
#         source_data = util_obj.extractallData(api_url)
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = 0
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     ########################################### function 2: writeExtractDataintoGCS #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "2_writeExtractDataintoGCS"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         ## call function writeExtractDataintoGCS for write data in bucket
#         # Initialize the GCP project ID
#         project_id = 'spark-learning-431506'
#         # Define the GCS bucket name where the data will be stored
#         load_data_bucket_name = 'earthquake_analysis_buck'
#         # Set the destination blob (file) name with a unique timestamp
#         destination_blob_name = f'pyspark/landing/{cur_timestamp}'
#         ## call function writeExtractDataintoGCS
#         util_obj.writeExtractDataintoGCS(project_id, source_data, load_data_bucket_name, destination_blob_name, api_url)
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = 0
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     ########################################### function 3: readDataFromLandingGcs #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "3_readDataFromLandingGcs"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         ## read data(json) from gcs bucket(from landing or bronze layer) by using readDataFromLandingGcs function
#         # Specify the GCS bucket and blob name
#         read_data_bucket_name = load_data_bucket_name
#         read_data_location = destination_blob_name
#         # call function readDataFromLandingGcs
#         json_data = util_obj.readDataFromLandingGcs(project_id, read_data_bucket_name, read_data_location)
#         # print(json_data,type(json_data)) ##dict
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = 0
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     ########################################### function 4: extractRequiredData #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "4_extractRequiredData"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         ## call extractRequiredData function for fetch required data
#         reuired_data_lst_of_dic = util_obj.extractRequiredData(json_data)
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = 0
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     ########################################### function 5: convertIntoDF #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "5_convertIntoDF"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         ## call convertIntoDF function for convert into dataframe
#         earthquake_dataframe = util_obj.convertIntoDF(spark, reuired_data_lst_of_dic)
#         # earthquake_dataframe.show()
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = earthquake_dataframe.count()
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#     #
#     # ########################################### function 4: writeIntoGcs #############################################################################################################
#     #
#     # ## information collect for audit log regarding extract data function
#     # try:
#     #     function_name = "4_writeIntoGcs"
#     #     start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #
#     #     ## call function writeIntoGcs for write data into gsc bucket (earthquake_analysis)
#     #
#     #     gcs_landing_location = f"gs://earthquake_analysis_buck/pyspark/landing/{cur_timestamp}"
#     #     # gcs_landing_location = f"D:/Mohini Data Science/earthquake_ingestion/bronze/landing_data/earthquake{cur_timestamp}"
#     #     util_obj.writeIntoGcs(earthquake_dataframe, gcs_landing_location)
#     #
#     #     end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #     status = "successful"
#     #     process_record = earthquake_dataframe.count()
#     #
#     # except:
#     #     end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #     status = "fail"
#     #     process_record = 0
#     #
#     # ## create audit data fram by using  createDFforAuditTbl function
#     # audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)
#     #
#     # ## call auditTblSchema function for get audit table schema
#     # audit_table_schema = util_obj.auditTblSchema()
#     #
#     # ## write audit data to bigquery by using writeDataBigquery function
#     # util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     #######################################################################################################################################################################
#
#     ## read data from gcs
#     gcs_input_location = gcs_landing_location
#
#     #
#     # ########################################### function 5: readDataFromloandingGCS #############################################################################################################
#     #
#     # ## information collect for audit log regarding extract data function
#     # try:
#     #     function_name = "5_readDataFromloandingGCS"
#     #     start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #
#     #     ##call readDataFromloandingGCS function for read data
#     #     earthquake_data = util_obj.readDataFromloandingGCS(spark, gcs_input_location)
#     #     # earthquake_data.show()
#     #     # earthquake_data.printSchema()
#     #
#     #     end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #     status = "successful"
#     #     process_record = earthquake_data.count()
#     #
#     # except:
#     #     end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #     status = "fail"
#     #     process_record = 0
#     #
#     # ## create audit data fram by using  createDFforAuditTbl function
#     # audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,process_record)
#     #
#     # ## call auditTblSchema function for get audit table schema
#     # audit_table_schema = util_obj.auditTblSchema()
#     #
#     # ## write audit data to bigquery by using writeDataBigquery function
#     # util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#     #
#     #
#
#     ########################################### function 6: flattenData #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "6_flattenData"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         ## call flattenData function for flattening the data
#         flatten_data_df = util_obj.flattenData(earthquake_data)
#         flatten_data_df.show(truncate=False)
#         # flatten_data_df.printSchema()
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = flatten_data_df.count()
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     ########################################### function 7: writeIntoGcs #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "7_writeIntoGcs"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#
#         ## upload flatten data in silver layer using  writeIntoGcs function from utils
#         output_path = f"gs://earthquake_analysis_buck/pyspark/silver/{cur_timestamp}"
#         # output_path = f"D:/Mohini Data Science/earthquake_ingestion/silver/intermediate_data/{cur_timestamp}"
#         util_obj.writeIntoGcs(flatten_data_df, output_path)
#
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "successful"
#         process_record = flatten_data_df.count()
#
#     except:
#         end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#         status = "fail"
#         process_record = 0
#
#     ## create audit data fram by using  createDFforAuditTbl function
#     audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
#                                             process_record)
#
#     ## call auditTblSchema function for get audit table schema
#     audit_table_schema = util_obj.auditTblSchema()
#
#     ## write audit data to bigquery by using writeDataBigquery function
#     util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
#
#     ########################################### function 8: writeDataBigquery #############################################################################################################
#
#     ## information collect for audit log regarding extract data function
#     try:
#         function_name = "8_writeDataBigquery"
#         start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
#     #
    #     ## write data in bigquery
    #     output_db = 'spark-learning-431506.earthquake_db.earthquake_data'
    #     util_obj.writeDataBigquery(output_db, flatten_data_df)
    #
    #     end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    #     status = "successful"
    #     process_record = flatten_data_df.count()
    #
    # except:
    #     end_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    #     status = "fail"
    #     process_record = 0
    #
    # ## create audit data fram by using  createDFforAuditTbl function
    # audit_df = util_obj.createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status,
    #                                         process_record)
    #
    # ## call auditTblSchema function for get audit table schema
    # audit_table_schema = util_obj.auditTblSchema()
    #
    # ## write audit data to bigquery by using writeDataBigquery function
    # util_obj.writeDataBigquery(audit_output_db, audit_df, audit_table_schema)
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
