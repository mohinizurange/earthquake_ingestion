from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType
# from util import dfSchema,extractallData,writeIntoGcs
from util import Utils
from datetime import datetime

# ## define extractRequiredData function
# def extractRequiredData(data):
#     ## fetch the metadata
#     metadata_dic = data["metadata"]
#
#     ## fetch count of records
#     cnt_rcd = metadata_dic['count']
#     print(f"total number of records {cnt_rcd}")
#
#     ## fetch the required data (features)
#     required_data = data['features']
#     # print(required_data,type(required_data)) ##list
#
#     reuired_data_lst = []
#     for dict in required_data:
#         ## fetch properties
#         properties_dic = dict["properties"]
#
#         ## add geometry cordinate in  properties
#         properties_dic["geometry"] = dict["geometry"]["coordinates"]
#
#         ## append properties_dic in list
#         reuired_data_lst.append(properties_dic)
#
#     # print(reuired_data_lst_of_dic)
#     return reuired_data_lst
#
# ## define convertIntoDF function
# def convertIntoDF(reuired_data_lst_of_dict,schema):
#     ## convert into df
#     earthquake_data = spark.createDataFrame(reuired_data_lst_of_dict, schema=schema)
#     # earthquake_data.show()
#     # earthquake_data.printSchema()
#     return earthquake_data

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## API uri
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    ##call class Utils
    util_obj = Utils()
    ## call extractData function for extract all data from api
    all_data = util_obj.extractallData(api_url)

    ## call extractRequiredData function for fetch required data
    reuired_data_lst_of_dic = util_obj.extractRequiredData(all_data)

    ## call convertIntoDF function for convert into dataframe
    earthquake_dataframe = util_obj.convertIntoDF(spark,reuired_data_lst_of_dic)
    earthquake_dataframe.show()

    ## call function writeIntoGcs for write data into gsc bucket (earthquake_analysis)
    # Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # gcs_landing_location= f"gs://earthquake_analysis_buck/pyspark/landing/{cur_timestamp}"
    gcs_landing_location = f"D:/Mohini Data Science/earthquake_ingestion/bronze/landing_data/earthquake{cur_timestamp}"
    util_obj.writeIntoGcs(earthquake_dataframe,gcs_landing_location)








