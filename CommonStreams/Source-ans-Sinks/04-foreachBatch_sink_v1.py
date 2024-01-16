from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

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

    countDf = fileStreamDF.groupBy(fileStreamDF.Country_Code).count()

    def process_batch(df, epochId):
        print(df, epochId)
        df.show()

        file_path = os.path.join("foreachBatch_dir", str(epochId))

        q1 = df.repartition(1) \
                .write.mode("errorifexists") \
                .csv(file_path)
        
    query = countDf.writeStream \
                    .foreachBatch(process_batch) \
                    .outputMode("update").start()
    
    query.awaitTermination()