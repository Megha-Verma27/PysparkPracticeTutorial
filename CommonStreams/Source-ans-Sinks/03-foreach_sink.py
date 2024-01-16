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

    process_dir = "./foreach_dir"

    def process_row(row):

        print("---------------------------------")
        print(row)

        file_path = os.path.join(process_dir, row["Country_Code"])

        with open(file_path, 'w') as f:
            f.write("%s, %s\n"% (row["Country_Code"], row["count"]))

    query = countDf.writeStream \
                    .foreach(process_row) \
                    .outputMode("complete") \
                    .start() \
                    .awaitTermination()