from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
from pyspark.sql import SparkSession
from util import Utils
from datetime import datetime
from pyspark.sql.functions import current_timestamp


def createDFforAuditTbl(spark,job_id,pipeline_name,function_name,start_time,end_time,status,process_record=0):
    audit_entry = [Row(job_id =job_id,
                       pipeline_name=pipeline_name,
                       function_name=function_name,
                       start_time=start_time,
                       end_time=end_time,
                       status=status,
                       process_record=process_record)]

    schema = StructType([
        StructField('job_id',StringType(),True),
        StructField('pipeline_name', StringType(), True),
        StructField('function_name', StringType(), True),
        StructField('start_time', StringType(), True),
        StructField('end_time', StringType(), True),
        StructField('status', StringType(), True),
        StructField('process_record', IntegerType(), True),

    ])


    # Create DataFrame with the provided schema
    audit_df = spark.createDataFrame(audit_entry, schema)

    # Show the DataFrame
    audit_df.show(truncate=False)
    audit_df.printSchema()
    return audit_df



# Initialize Spark session
spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

##call class Utils
util_obj = Utils()

## initializtion of temp bucket for storing stagging data
bucket = "earthquake_dataproc_temp_bucket"
spark.conf.set("temporaryGcsBucket", bucket)

cur_timestamp =datetime.now().strftime('%Y%m%d_%H%M%S')
job_id = cur_timestamp
try:
    pipeline_name= "earthquake_pipeline_dev"
    function_name="extractallData"
    start_time = cur_timestamp
    ##call your function
    end_time = cur_timestamp
    status = "successful"

except:
    end_time = current_timestamp()
    status = "fail"
    # process_record =

audit_df =createDFforAuditTbl(spark,job_id,pipeline_name,function_name,start_time,end_time,status)

## write audit data in bigquery
output_db = 'spark-learning-431506.earthquake_db.earthquake_audit_tbl'

audit_table_schema = """
    job_id:STRING,
    pipeline_name:STRING,
    function_name:STRING,
    start_time:STRING,  
    end_time:STRING,    
    status:STRING,
    process_record:INTEGER
"""

util_obj.writeDataBigquery(output_db,audit_df,audit_table_schema)