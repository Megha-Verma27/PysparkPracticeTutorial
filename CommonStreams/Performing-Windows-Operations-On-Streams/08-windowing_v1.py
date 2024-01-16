from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, window

if __name__ == "__main__":
    sparkSession  = SparkSession.builder.appName("Windowing Operations").getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    schema = StructType([StructField("Date", TimestampType(), True),
                         StructField("Open", DoubleType(), True),
                         StructField("High", DoubleType(), True),
                         StructField("Low", DoubleType(), True),
                         StructField("Close", DoubleType(), True),
                         StructField("Adjusted Close", DoubleType(), True),
                         StructField("Volume", DoubleType(), True),
                         StructField("Name", StringType(), True)
                         ])
    
    stockPricesDf = sparkSession.readStream.option("header", "true").schema(schema)\
                    .csv("datasets/stockPricesDataset/droplocation")
    
    print(" ")
    print(stockPricesDf.printSchema())


    averageCloseDf = stockPricesDf.groupBy("Name").agg({"Close": "avg"}).withColumnRenamed("avg(Close)", "Average_Close")

    query = averageCloseDf.writeStream.outputMode("complete").format("console") \
                            . option("truncate", "false").start() \
                            .awaitTermination()
    

