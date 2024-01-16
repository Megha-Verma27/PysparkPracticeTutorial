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

    consoleQuery = (
        inputDF
        .writeStream
        .queryName("FhvRidesQuery")
        .format("console")
        .outputMode("append")
        .trigger(processingTime = '10 seconds')
        .start()
    )

    consoleQuery.awaitTermination()