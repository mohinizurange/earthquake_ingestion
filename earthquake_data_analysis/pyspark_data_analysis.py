from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,avg,date_format,date_sub,current_date,max
import os
if __name__ == '__main__':
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"D:\Mohini Data Science\GCP\Python_gcp\spark-learning-431506-160e4ffdff74_key.json"

    spark = SparkSession.builder.master('local[*]').appName('earthquake_data_analysis').getOrCreate()
    eq_df = spark.read.json(r"D:\Mohini Data Science\earthquake_ingestion\silver\intermediate_data\20241025_111111")
    # eq_df.show()
    # eq_df.printSchema()

    # 1. Count the number of earthquakes by region
    num_of_earthquake_by_region = eq_df.groupBy(col('area')).agg(count(col('ids')).alias('cunt'))
    # num_of_earthquake_by_region.show()

    # 2. Find the average magnitude by the region
    avg_mag_by_region = eq_df.groupBy(col('area')).agg(avg(col('mag')).alias('avg_mag'))
    # avg_mag_by_region.show()

    # 3. Find how many earthquakes happen on the same day.
    num_eq_on_same_day = eq_df.withColumn('date',date_format(col('time'),'yyyy-MM-dd'))\
                          .groupBy(col('date')).agg(count(col('ids')).alias('cunt'))
    # num_eq_on_same_day.show()


    # 4. Find how many earthquakes happen on same day and in same region
    num_eq_on_same_day_same_region = eq_df.withColumn('date',date_format(col('time'),'yyyy-MM-dd'))\
                          .groupBy(col('date'),col('area')).agg(count(col('ids')).alias('cunt'))
    # num_eq_on_same_day_same_region.show()

    # 5. Find average earthquakes happen on the same day.
    num_eq_on_same_day = eq_df.withColumn('date', date_format(col('time'), 'yyyy-MM-dd')) \
        .groupBy(col('date')).agg(count(col('ids')).alias('cunt'))

    avg_eq_df = num_eq_on_same_day.agg(avg(col('cunt')).alias('avg_earthquake'))
    # avg_eq_df.show()

    # 6. Find average earthquakes happen on same day and in same region
    num_eq_on_same_day_same_region = eq_df.withColumn('date', date_format(col('time'), 'yyyy-MM-dd')) \
        .groupBy(col('date'), col('area')).agg(count(col('ids')).alias('cunt'))

    avg_eq = num_eq_on_same_day_same_region.agg(avg(col('cunt')).alias('avg_earthquake'))
    # avg_eq.show()

    # 7. Find the region name, which had the highest magnitude earthquake last week.
    highest_mag_df = (eq_df.withColumn('date', date_format(col('time'), 'yyyy-MM-dd'))
                   .filter(col('date') >= date_sub(current_date(), 7)) \
                   .agg(max(col('mag')).alias('highest_mag')))
    # highest_mag_df.show()
    # print(highest_mag.collect())
    highest_mag = highest_mag_df.collect()[0][0]
    # print(highest_mag)
    highest_mag_region_name = eq_df.select('area','mag').filter(col('mag')==highest_mag)
    # highest_mag_region_name.show()

    # 8. Find the region name, which is having magnitudes higher than 5.
    region_name_mag_higher_than_5 = eq_df.select('area','mag').filter(col('mag')>5)
    # region_name_mag_higher_than_5.show()

    # 9. Find out the regions which are having the highest frequency and intensity of earthquakes.
    result =(eq_df
     .filter(col("mag").isNotNull())
     .groupBy("area")
     .agg(count("*").alias("earthquake_count"),
          avg("mag").alias("average_magnitude"))
     .orderBy(col("earthquake_count").desc(),
              col("average_magnitude").desc()))
    result.show()