
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
import requests
from datetime import datetime
## define extractallData function
def extractallData(api_url):
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


##flatten data frame schema

def flattenDataSchema():
    ### Define the schema

    DF_schema = StructType([
        StructField("mag", StringType(), True),
        StructField("place", StringType(), True),
        StructField("time", TimestampType(), True),
        StructField("updated", TimestampType(), True),
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
        StructField("area", StringType(), True),
        StructField("longtitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True),
        StructField("insert_date", TimestampType(), True)


    ])
    return  DF_schema


## define writeIntoGcs function
def writeIntoGcs(earthquake_df,out_path):
    # Get the current date and time in 'YYYYMMDD_HHMMSS' format
    current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ## write data into gcs
    output_path = f"{out_path}/earthquake{current_timestamp}"
    earthquake_df.coalesce(2).write.mode('overwrite').json(output_path)
    print(f"data write successfully in {output_path}")