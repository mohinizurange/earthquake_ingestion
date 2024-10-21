from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType
from util import dfSchema ,extractallData,writeIntoGcs


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

    # print(reuired_data_lst_of_dic)
    return reuired_data_lst

## define convertIntoDF function
def convertIntoDF(reuired_data_lst_of_dict,schema):
    ## convert into df
    earthquake_data = spark.createDataFrame(reuired_data_lst_of_dict, schema=schema)
    # earthquake_data.show()
    # earthquake_data.printSchema()
    return earthquake_data

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("extarct_the_data_from_API").getOrCreate()

    ## API uri
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    ## call extractData function for extract all data
    all_data = extractallData(api_url)

    ## call extractRequiredData function for fetch required data
    reuired_data_lst_of_dic = extractRequiredData(all_data)

    ## call function dfSchema
    data_frame_schema = dfSchema()

    ## call convertIntoDF function for convert into dataframe
    earthquake_dataframe = convertIntoDF(reuired_data_lst_of_dic,data_frame_schema)
    earthquake_dataframe.show()

    ## call function writeIntoGcs to write data into gsc bucket (earthquake_analysis)
    output_path=r"gs://earthquake_analysis_buck/pyspark/landing"
    # output_path = r"D:/Mohini Data Science/earthquake_ingestion/bronze/landing_data"
    writeIntoGcs(earthquake_dataframe,output_path)








