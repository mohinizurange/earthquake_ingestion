from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType

import requests


if __name__ == '__main__':
    # Initialize Spark session

    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## API uri
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    ## by using get method extract the data from api

    response = requests.get(api_url)

    ##Check if the request was successful

    if response.status_code==200:
        ##convert data into json
        data = response.json()
        # print("Extracted Data:", data)

        ## fetch the metadata
        metadata_dic = data["metadata"]

        ## fetch count of records
        cnt_rcd = metadata_dic['count']
        print(cnt_rcd)

        ## fetch the required data (features)
        required_data = data['features']
        # print(required_data,type(required_data)) ##list

        reuired_data_lst_of_dic = []

        for dict in required_data:

            ## fetch properties
            properties_dic = dict["properties"]

            ## add geometry cordinate in  properties
            properties_dic["geometry"]=dict["geometry"]["coordinates"]

            ## add id in properties

            properties_dic["id"]= dict["id"]

            ## append properties_dic in list

            reuired_data_lst_of_dic.append(properties_dic)

        # print(reuired_data_lst_of_dic)

        ### Define the schema

        schema = StructType([
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
            StructField("geometry", ArrayType(StringType()), True),
            StructField("id", StringType(), True)

        ])

        ## convert into df
        earthquake_data = spark.createDataFrame(reuired_data_lst_of_dic,schema= schema)
        earthquake_data.show()
        # earthquake_data.printSchema()

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")


