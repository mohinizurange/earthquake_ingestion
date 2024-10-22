from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,substring,instr,length,expr
from util import Utils
from datetime import datetime
from google.cloud import bigquery

# def readDataFromloandingGCS(spark,input_path,schema):
#     earthquake_df = spark.read.json(input_path, schema=schema)
#     return earthquake_df
# def flattenData(earthquake_df):
#     ## flatten the data
#     ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
#     ## Using split() to extract area and Generate column “area” -
#     ## add one cloumn insert date
#
#     flatten_data_df = (earthquake_df
#                        .withColumn('time', to_timestamp(from_unixtime(col('time') / 1000)))
#                        .withColumn('updated', to_timestamp(from_unixtime(col('updated') / 1000)))
#                        .withColumn('area', expr("substring(place, instr(place, 'of') + 3, length(place))"))
#                        .withColumn('longtitude', col('geometry').getItem(0).cast('float'))
#                        .withColumn('latitude', col('geometry').getItem(1).cast('float'))
#                        .withColumn('depth', col('geometry').getItem(2).cast('float'))
#                        .withColumn('insert_date',current_timestamp()) ##  we can also use  lit(insert_date )= datetime.now().strftime('%Y%m%d %H%M%S')
#                        .drop(col("geometry"))
#
#                        )
#
#     return flatten_data_df

## define function for write data in bigquery
# def writeDataBigquery(output_db,flatten_data_df,bq_schema):
#     print(f'{flatten_data_df.count() }: no of records ')
#     flatten_data_df.write.format('bigquery').option("table",output_db)\
#                                             .option("schema",bq_schema)\
#                                             .option("createDisposition","CREATE_IF_NEEDED")\
#                                             .option("writeDisposition", "WRITE_APPEND") \
#                                             .mode('append')\
#                                             .save()
#     print(f"load data successfully in {output_db}")



if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## initializtion of temp bucket for storing stagging data
    bucket = "earthquake_dataproc_temp_bucket"
    spark.conf.set("temporaryGcsBucket", bucket)

    ##call class Utils
    util_obj = Utils()

    ## read data from gcs
    # input_path  =r"D:\Mohini Data Science\earthquake_ingestion\bronze\landing_data\earthquake20241021_193355"
    # input_path= r"gs://earthquake_analysis_buck/pyspark/landing/earthquake20241021_165143"
    ##call readDataFromBronzeGCS function for read data
    earthquake_data = util_obj.readDataFromloandingGCS(spark,input_path)

    ## call flattenData function for flattening the data
    flatten_data_df= util_obj.flattenData(earthquake_data)
    flatten_data_df.show(truncate=False)
    flatten_data_df.printSchema()

    ## upload flatten data in silver layer using  writeIntoGcs function from utils
    # Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # output_path=f"gs://earthquake_analysis_buck/pyspark/silver/{cur_timestamp}"
    gcs_silver_layer_location = f"D:/Mohini Data Science/earthquake_ingestion/silver/intermediate_data/{cur_timestamp}"
    util_obj.writeIntoGcs(flatten_data_df,gcs_silver_layer_location)

    ## write data in bigquery
    output_db = 'spark-learning-431506.earthquake_db.earthquake_data'
    # util_obj.writeDataBigquery(output_db)