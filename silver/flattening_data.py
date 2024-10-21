from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp
from util import dfSchema,writeIntoGcs,flattenDataSchema
from datetime import datetime
from google.cloud import bigquery

def readDataFromBronzeGCS(spark,input_path,schema):
    earthquake_df = spark.read.json(input_path, schema=schema)
    return earthquake_df
def flattenData(earthquake_df):
    ## flatten the data
    ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
    ## Using split() to extract area and Generate column “area” -
    ## add one cloumn insert date

    flatten_data_df = (earthquake_df
                       .withColumn('time', to_timestamp(from_unixtime(col('time') / 1000)))
                       .withColumn('updated', to_timestamp(from_unixtime(col('updated') / 1000)))
                       .withColumn('area', trim(split(col('place'), 'of')[1]))
                       .withColumn('longtitude', col('geometry').getItem(0).cast('float'))
                       .withColumn('latitude', col('geometry').getItem(1).cast('float'))
                       .withColumn('depth', col('geometry').getItem(2).cast('float'))
                       .withColumn('insert_date',current_timestamp()) ##  we can also use  lit(insert_date )= datetime.now().strftime('%Y%m%d %H%M%S')
                       .drop(col("geometry"))

                       )
    return flatten_data_df

## define function for write data in bigquery
def writeDataBigquery(output_db,flatten_data_df,flatten_schema):
    print(f'{flatten_data_df.count() }: no of records ')
    flatten_data_df.write.format('bigquery').option("table",output_db)\
                                            .option("createDisposition","CREATE_IF_NEEDED")\
                                            .option("writeDisposition", "WRITE_APPEND") \
                                            .mode('append')\
                                            .save()



if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## initializtion of temp bucket for storing stagging data
    bucket = "earthquake_dataproc_temp_bucket"
    spark.conf.set("temporaryGcsBucket", bucket)

    ## call dfschema function(util) to get schema
    schema=dfSchema()

    ## read data from gcs
    # input_path  =r"D:\Mohini Data Science\earthquake_ingestion\bronze\landing_data\earthquake20241021_193355"
    input_path= r"gs://earthquake_analysis_buck/pyspark/landing/earthquake20241021_165143"
    ##call readDataFromBronzeGCS function for read data
    earthquake_data = readDataFromBronzeGCS(spark,input_path,schema)

    ## call flattenData function for flattening the data
    flatten_data_df=flattenData(earthquake_data)
    flatten_data_df.show(truncate=False)
    flatten_data_df.printSchema()

    ## upload flatten data in silver layer using  writeIntoGcs function from utils
    output_path=r"gs://earthquake_analysis_buck/pyspark/silver"
    # output_path = r"D:/Mohini Data Science/earthquake_ingestion/silver/intermediate_data"
    # writeIntoGcs(flatten_data_df,output_path)

    ## write data in bigquery
    output_db = 'spark-learning-431506.earthquake_db.earthquake_data'
    flatten_schema = flattenDataSchema()
    writeDataBigquery(output_db,flatten_data_df,flatten_schema)