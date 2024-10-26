
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,substring,instr,length,expr
from pyspark.sql import Row
import requests
from datetime import datetime
from google.cloud import bigquery,storage
import json

class Utils():
    """
       Utility class for handling data extraction, transformation, and loading operations.

       Overall Information:
       This class provides methods for:
       - Extracting data from APIs
       - Writing and reading data to/from Google Cloud Storage (GCS)
       - Transforming data into Spark DataFrames
       - Flattening DataFrames
       - Defining schemas for Spark DataFrames and BigQuery
       """
    ## define extractallData function
    def extractallData(self,api_url):
        """
                Extracts data from the specified API URL.

                Parameters:
                    api_url (str): The URL of the API to extract data from.

                Returns:
                    str: A JSON string of the extracted data if successful; otherwise, None.
                """

        ## by using get method extract the data from api
        response = requests.get(api_url)

        ##Check if the request was successful
        if response.status_code == 200:
            ##convert data into json
            all_data = response.json()  # converts the (api)JSON response data into Python data types (usually a dictionary or a list).
            # print("Extracted Data:", all_data)
            print(f"extract data successfully from {api_url}")
            return json.dumps(all_data)  # Convert the Python dictionary to a JSON string

        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return None

    def writeExtractDataintoGCS(self,project_id, source_data, bucket_name, destination_blob_name, api_url):
        """
                Writes the extracted data into a Google Cloud Storage (GCS) bucket.

                Parameters:
                    project_id (str): The GCP project ID.
                    source_data (str): The data to be written to GCS.
                    bucket_name (str): The name of the GCS bucket.
                    destination_blob_name (str): The name of the destination blob in GCS.

                Returns:
                    None
                """
        # Create a GCS client using the specified project ID
        client = storage.Client(project=project_id)

        # Get the GCS bucket object
        bucket_obj = client.bucket(bucket_name)

        # Create a new blob (file) in the bucket with the specified name
        blob = bucket_obj.blob(destination_blob_name)

        # Upload the extracted data to the GCS bucket
        blob.upload_from_string(
            data=source_data,
            content_type='application/json', timeout=100
        )
        print(f" write data successfully in {bucket_name}/{destination_blob_name}")

    ## define function readDataFromLandingGcs for read data from gcs bucket(from landing or bronze layer)

    def readDataFromLandingGcs(self,project_id, bucket_name, read_data_location):
        """
                Reads data from the landing GCS bucket.

                Parameters:
                    project_id (str): The GCP project ID.
                    bucket_name (str): The name of the GCS bucket.
                    read_data_location (str): The path to the data in the GCS bucket.

                Returns:
                    dict: The JSON data as a Python dictionary.
                """
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

    ## define extractRequiredData function
    def extractRequiredData(self,data):
        """
                Extracts required data features from the given JSON data.

                Parameters:
                    data (dict): The JSON data from which to extract features.

                Returns:
                    list: A list of dictionaries containing the required data features.
                """

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

        # print(reuired_data_lst)
        return reuired_data_lst

    def dfSchema(self):
        """
                Defines the schema for the DataFrame.

                Returns:
                    StructType: The schema definition for the DataFrame.
                """
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

    ## define convertIntoDF function
    def convertIntoDF(self,spark,reuired_data_lst_of_dict,data_frame_schema = None):
        """
                Converts a list of dictionaries into a Spark DataFrame.

                Parameters:
                    spark: The Spark session instance.
                    required_data_lst_of_dict (list): A list of dictionaries to convert into DataFrame.
                    data_frame_schema (StructType, optional): The schema for the DataFrame.

                Returns:
                    DataFrame: The resulting DataFrame.
                """
        if data_frame_schema is None:
            ## call function dfSchema to get a schema
            data_frame_schema = self.dfSchema()
        ## convert into df
        earthquake_data = spark.createDataFrame(reuired_data_lst_of_dict, schema=data_frame_schema)
        # earthquake_data.show()
        # earthquake_data.printSchema()
        return earthquake_data

    ## define writeIntoGcs function for write data in gcs bucket
    def writeIntoGcs(self, earthquake_df, output_path):
        """
        Writes a DataFrame into a GCS bucket as JSON.

        Parameters:
            earthquake_df: The DataFrame to write.
            output_path (str): The output path in the GCS bucket.

        Returns:
            None
        """

        # Attempt to write the DataFrame to JSON
        earthquake_df.coalesce(2).write.mode('overwrite').json(output_path)
        print(f"data write successfully in {output_path}")

    def flattenData(self,earthquake_df):
        """
                Flattens the earthquake DataFrame and transforms certain columns.

                Parameters:
                    earthquake_df: The DataFrame to flatten.

                Returns:
                    DataFrame: The flattened DataFrame with transformed columns.
                """
        ## flatten the data
        ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
        ## Using split() to extract area and Generate column “area” -
        ## add one cloumn insert date

        flatten_data_df = (earthquake_df
                           .withColumn('time', to_timestamp(from_unixtime(col('time') / 1000)))
                           .withColumn('updated', to_timestamp(from_unixtime(col('updated') / 1000)))
                           .withColumn('area', expr("substring(place, instr(place, 'of') + 3, length(place))"))
                           .withColumn('longtitude', col('geometry').getItem(0).cast('float'))
                           .withColumn('latitude', col('geometry').getItem(1).cast('float'))
                           .withColumn('depth', col('geometry').getItem(2).cast('float'))
                           .withColumn('insert_date',
                                       current_timestamp())  ##  we can also use  lit(insert_date )= datetime.now().strftime('%Y%m%d %H%M%S')
                           .drop(col("geometry"))

                           )

        return flatten_data_df

        ## schema for bigquery

    def bqSchema(self):
        """
                Defines the schema for BigQuery.

                Returns:
                    list: A list of dictionaries representing the BigQuery schema.
                """
        # Define the schema with mode
        bq_schema = [
            {"name": "mag", "type": "STRING", "mode": "NULLABLE"},
            {"name": "place", "type": "STRING", "mode": "NULLABLE"},
            {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "tz", "type": "STRING", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
            {"name": "felt", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cdi", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mmi", "type": "STRING", "mode": "NULLABLE"},
            {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "net", "type": "STRING", "mode": "NULLABLE"},
            {"name": "code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
            {"name": "types", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dmin", "type": "STRING", "mode": "NULLABLE"},
            {"name": "rms", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gap", "type": "STRING", "mode": "NULLABLE"},
            {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "longtitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "insert_date", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]

        return bq_schema

    ## define function for write data in bigquery
    def writeDataBigquery(self,output_db, data_df,bq_schema=None):
        """
                Writes data from a DataFrame to a specified BigQuery table.

                Parameters:
                    output_db (str): The target BigQuery table in the format 'project_id.dataset_id.table_id'.
                    data_df (DataFrame): The DataFrame containing the data to be written.
                    bq_schema (list, optional): The schema for the BigQuery table. If None, the schema will be fetched using the bqSchema method.

                Returns:
                    None
        """
        if bq_schema is None:
            ##call function bqSchema to get bq schema
            bq_schema = self.bqSchema()

        print(f'{data_df.count()}: no of records ')

        data_df.write.format('bigquery').option("table", output_db) \
            .option("schema", bq_schema) \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_APPEND") \
            .mode('append') \
            .save()
        print(f"load data successfully in {output_db}")


    ## define createDFforAuditTbl function for create df for audit data
    def createDFforAuditTbl(self,spark_1, job_id, pipeline_name, function_name, start_time, end_time, status,
                            process_record=0):
        """
                Creates a DataFrame for audit logs with job execution details.

                Parameters:
                    spark_1 (SparkSession): The Spark session to create a DataFrame.
                    job_id (str): Unique identifier for the job.
                    pipeline_name (str): Name of the data pipeline.
                    function_name (str): Name of the function executing the job.
                    start_time (str): Start time of the job execution.
                    end_time (str): End time of the job execution.
                    status (str): Status of the job execution (e.g., SUCCESS, FAILURE).
                    process_record (int, optional): Number of records processed. Defaults to 0.

                Returns:
                    DataFrame: A DataFrame containing the audit log entry.
        """

        audit_entry = [Row(job_id=job_id,
                           pipeline_name=pipeline_name,
                           function_name=function_name,
                           start_time=start_time,
                           end_time=end_time,
                           status=status,
                           process_record=process_record)]

        schema = StructType([
            StructField('job_id', StringType(), True),
            StructField('pipeline_name', StringType(), True),
            StructField('function_name', StringType(), True),
            StructField('start_time', StringType(), True),
            StructField('end_time', StringType(), True),
            StructField('status', StringType(), True),
            StructField('process_record', IntegerType(), True),

        ])

        # Create DataFrame with the provided schema
        audit_df = spark_1.createDataFrame(audit_entry, schema)

        # Show the DataFrame
        # audit_df.show(truncate=False)
        # audit_df.printSchema()
        return audit_df

    ### defind function for create audit tbl schema
    def auditTblSchema(self):
        """
                Defines the schema for the audit table in BigQuery.

                Returns:
                    list: A list of dictionaries defining the schema for the audit table.
        """
        audit_table_schema = [
            {"name": "job_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pipeline_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "function_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "start_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "end_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "process_record", "type": "INTEGER", "mode": "NULLABLE"}
        ]
        return audit_table_schema






