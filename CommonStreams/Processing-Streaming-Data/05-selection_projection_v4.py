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

    age_category_column = expr("IF(price > 70000, 'premium', 'regular')")

    projectionsDf = fileStreamDf.select(fileStreamDf.car,
                                        fileStreamDf.model,
                                        fileStreamDf.price,
                                        fileStreamDf.mileage,
                                        fileStreamDf.year) \
                                        .where(fileStreamDf.year > 2014) \
                                        .filter("price != 0") \
                                        .withColumn("age_category", age_category_column)
    query = projectionsDf.writeStream \
                        .outputMode("append") \
                        .format("console") \
                        .option("numRows", 10)\
                        .start()
    
    query.awaitTermination()