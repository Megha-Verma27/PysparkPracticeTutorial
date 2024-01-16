from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = (
        SparkSession
        .builder
        .appName("SparkStructuredStreamingApp")
        .master("local[4]")
        .config("spark.dynamicAllocation.enabled", "false")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Define schema for FHV Rides
    rideSchema = (
                    StructType()
                        .add("Id", "integer")
                        .add("VendorId", "integer")
                        .add("PickupTime", "timestamp")
                        .add("DropTime", "timestamp")
                        .add("PickupLocationId", "integer")
                        .add("DropLocationId", "integer")
                        .add("CabLicense", "string")
                        .add("DriverLicense", "string")
                        .add("PassengerCount", "integer")
                        .add("RateCodeId", "integer")
                )
    
    inputDF = (
        spark
        .readStream
        .schema(rideSchema)
        .option("maxFilesPerTrigger", 1)
        .option("multiline", "true")
        .json("C:\DataFiles\Streaming")
    )

    print("Is inputDF streaming DataFrame = " + str(inputDF.isStreaming))

    tripTypeColumn = (
        when(
            col("RatecodeID") == 6,
            "SharedTrip"
        )
        .otherwise("SoloTrip")
    )

    transformedDF = (
        inputDF
        .select(
            "VendorId", "PickupTime", "DropTime", "PickupLocationId",
            "DropLocationId", "PassengerCount", "RateCodeId"
        )
        .where("PassengerCount > 0")
        .withColumn("TripTimeInMinutes", 
                    round(
                        (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime")))/60
                    )
                    )
        .withColumn("TripType", tripTypeColumn) 
        .drop("RatecodeID")
    )


    fileQuery = (
        transformedDF
        .writeStream
        .queryName("FhvRidesFileQuery")
        .format("csv")
        .option("path", "c:\DataFiles\StreamingOutput\Output")
        .option("checkpointLocation", "c:\DataFiles\StreamingOutput\CheckPoint")
        .outputMode("append")
        .trigger(processingTime = '5 seconds')
        .start()
    )


    transformedDF.createOrReplaceTempView("TaxiData")

    sqlDF = spark.sql("""
                        select PickupLocationId, count(*) as count from TaxiData group by PickupLocationId
                    """)
    sqlQuery = (
        sqlDF
        .writeStream
        .queryName("FhvRideSqlQuery")
        .format("console")
        .outputMode("complete")
        .trigger(processingTime = '10 seconds')
        .start()
    )

    sqlQuery.awaitTermination()

    

