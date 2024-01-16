from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("Console sink").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    schema = StructType([StructField("Date", StringType(), False),
                        StructField("Article_ID", StringType(), False),
                        StructField("Country_Code", StringType(), False),
                        StructField("Sold_Units", IntegerType(), False),
                         ]
                        )
    
    fileStreamDf = spark.readStream.option("header", "true").schema(schema).csv("datasets/historicalDataset/dropLocation")

    print(" ")
    print("Is the stream ready? ", fileStreamDf.isStreaming)

    print(" ")
    print("Stream schema ", fileStreamDf.printSchema())

    selectedDf = fileStreamDf.select("*")

    query = selectedDf.writeStream.outputMode("append").format("console").option("numRows", 10).start()

    query.awaitTermination()