from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':

    SparkSession = SparkSession.builder.master("local")\
    .appName("Console sink")\
    .getOrCreate()


    SparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("Date",StringType(),False),\
                            StructField("Article_ID",StringType(),False),\
                            StructField("Country_Code",StringType(),False),\
                            StructField("Sold_Units",IntegerType(),False),\
                            ])

    fileStreamDF = SparkSession.readStream \
                                .option("header","true")\
                                .schema(schema)\
                                .csv("datasets/historicalDataset/dropLocation")
    

    print(" ")
    print("Is thestream ready ? ", fileStreamDF.isStreaming)
    print(" ")
    print("Stream schema", fileStreamDF.printSchema())


    selectedDf = fileStreamDF.select("*")

    projectedDf = selectedDf.select(selectedDf.Date, selectedDf.Country_Code, selectedDf.Sold_Units)\
                            .withColumnRenamed("Date","date")\
                            .withColumnRenamed("Country_Code","countryCode")\
                            .withColumnRenamed("Sold_Units","soldUnits")

    query = projectedDf.writeStream \
                        .outputMode("append")\
                                .format("json")\
                                .option("path","C:/CommonStreams/Source-ans-Sinks/datasets/historicalDataset/filesink_results")\
                                .option("checkpointLocation","C:/CommonStreams/Source-ans-Sinks/datasets/historicalDataset/filesink_checkpoint")\
                                .start()
            
    query.awaitTermination()






    # print(" ")
    # print("Is the stream ready? ", fileStreamDf.isStreaming)

    # print(" ")
    # print("Stream schema ", fileStreamDf.printSchema())

    # selectedDf = fileStreamDf.select("*")

    # projectionDf = selectedDf.select(selectedDf.Date, selectedDf.Country_Code, selectedDf.Sold_Units)
    # .withColumnRenamed("Date", "date")
    # .withColumnRenamed("Country_Code", "countryCode")
    # .withColumnRenamed("Sold_Units", "soldUnits")

    # query = projectionDf.writeStream
    #                     .outputMode("append")
    #                     .format("json")
    #                     .opion("path", "filesink_results")
    #                     .option("checkpointLocation", "filesink_checkpoint")
    #                     .start()
    # query.awaitTermination()