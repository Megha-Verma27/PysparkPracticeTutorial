from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    sparkSession = SparkSession.builder.master("local") \
                               .appName("Selections and Projections") \
                               .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("car", StringType(), True),\
    					 StructField("price", DoubleType(), True),\
    					 StructField("body", StringType(), True),\
    					 StructField("mileage", DoubleType(), True),\
    					 StructField("engV", StringType(), True),\
    					 StructField("engType", StringType(), True),\
    					 StructField("registration", StringType(), True),\
    					 StructField("year", IntegerType(), True),\
    					 StructField("model", StringType(), True),\
    					 StructField("drive", StringType(), True)])
    
    fileStreamDf = sparkSession.readStream \
                                .option("header", "true") \
                                .option("maxFilesPerTrigger", 1) \
                                .schema(schema) \
                                .csv("datasets/carAdsDataset/droplocation")
    fileStreamDf.printSchema()
    aggregationDf = sparkSession.sql("""
                            select car as make, avg(price), min(price), max(price) from car_ads where year>2013 group by make
                            """)
    
    query = aggregationDf.writeStream \
                        .outputMode("complete") \
                        .format("console") \
                        .option("numRows", 10)\
                        .start()
    
    query.awaitTermination()