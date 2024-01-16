import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    sparkSession  = SparkSession.builder.appName("Windowing udf Operations").getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")



    schema = StructType([StructField("Invoice ID", StringType(), True),\
    					 StructField("Branch", StringType(), True),\
    					 StructField("City", StringType(), True),\
    					 StructField("Customer type", StringType(), True),\
    					 StructField("Gender", StringType(), True),\
    					 StructField("Product line", StringType(), True),\
    					 StructField("Unit price", DoubleType(), True),\
    					 StructField("Quantity", IntegerType(), True),\
    					 StructField("Tax 5%", DoubleType(), True),\
                         StructField("Total", DoubleType(), True),\
                         StructField("Date", StringType(), True),\
                         StructField("Time", StringType(), True),\
                         StructField("Payment", StringType(), True),\
                         StructField("cogs", DoubleType(), True),\
                         StructField("gross margin percentage", DoubleType(), True),\
                         StructField("gross income", DoubleType(), True),\
    					 StructField("Rating", DoubleType(), True)])
    
    fileStreamDf = sparkSession.readStream \
                                .option("header", "true")\
                                .schema(schema).csv("datasets/supermarketSalesDataset/dropLocation")
    fileStreamDf.printSchema()

    fileStreamDf = fileStreamDf.withColumn("ingestion_timestamp", current_timestamp())


    averageRatingDf = fileStreamDf.withWatermark("ingestion_timestamp", "1 minutes") \
                                  .groupBy(window(fileStreamDf.ingestion_timestamp, "1 minutes",  "30 seconds"), 
                                           fileStreamDf.City, fileStreamDf.Branch)\
                                  .agg({"Rating": "avg"})\
                                  .withColumnRenamed("avg(rating)", "avg_rating")\
                                  .orderBy("avg_rating", "City")
    

    query = averageRatingDf.writeStream\
                           .outputMode("complete")\
                           .format("console")\
                           .option("truncate", "false").start().awaitTermination()
    
