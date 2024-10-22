
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,substring,instr,length,expr
from pyspark.sql import Row
import requests
from datetime import datetime
from google.cloud import bigquery

class Utils():
    ## define extractallData function
    def extractallData(self,api_url):
        ## by using get method extract the data from api
        response = requests.get(api_url)

        ##Check if the request was successful
        if response.status_code == 200:
            ##convert data into json
            all_data = response.json()
            # print("Extracted Data:", data)
            return all_data
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")

    ## define extractRequiredData function
    def extractRequiredData(self,data):
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

        # print(reuired_data_lst_of_dic)
        return reuired_data_lst

    def dfSchema(self):
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

        # Attempt to write the DataFrame to JSON
        earthquake_df.coalesce(2).write.mode('overwrite').json(output_path)
        print(f"data write successfully in {output_path}")



    ## define function readDataFromloandingGCS for read data from gcs bucket
    def readDataFromloandingGCS(self,spark, input_path):
        ## call function dfSchema to get a schema
        data_frame_schema = self.dfSchema()

        # Attempt to read the JSON file
        earthquake_df = spark.read.json(input_path, schema=data_frame_schema)
        print("read data successfully.")
        return earthquake_df


    def flattenData(self,earthquake_df):
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
    ### Define the schema

        bq_schema = """
        mag:STRING,
        place:STRING,
        time:TIMESTAMP,
        updated:TIMESTAMP,
        tz:STRING,
        url:STRING,
        detail:STRING,
        felt:STRING,
        cdi:STRING,
        mmi:STRING,
        alert:STRING,
        status:STRING,
        tsunami:INTEGER,
        sig:INTEGER,
        net:STRING,
        code:STRING,
        ids:STRING,
        sources:STRING,
        types:STRING,
        nst:INTEGER,
        dmin:STRING,
        rms:STRING,
        gap:STRING,
        magType:STRING,
        type:STRING,
        title:STRING,
        area:STRING,
        longtitude:FLOAT,
        latitude:FLOAT,
        depth:FLOAT,
        insert_date:TIMESTAMP
        """

        return bq_schema


    ## define function for write data in bigquery
    def writeDataBigquery(self,output_db, data_df,bq_schema=None):
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
        audit_table_schema  = """
            job_id:STRING,
            pipeline_name:STRING,
            function_name:STRING,
            start_time:STRING,  
            end_time:STRING,    
            status:STRING,
            process_record:INTEGER
        """
        return audit_table_schema






