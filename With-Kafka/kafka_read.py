# Create the Spark Session
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()

    # Create the streaming_df to read from kafka
    streaming_df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "devices") \
        .option("startingOffsets", "earliest") \
        .load()

    # JSON Schema
    from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
    json_schema = StructType([StructField('customerId', StringType(), True), \
    StructField('data', StructType([StructField('devices', ArrayType(StructType([ \
    StructField('deviceId', StringType(), True), \
    StructField('measure', StringType(), True), \
    StructField('status', StringType(), True), \
    StructField('temperature', LongType(), True)]), True), True)]), True), \
    StructField('eventId', StringType(), True), \
    StructField('eventOffset', LongType(), True), \
    StructField('eventPublisher', StringType(), True), \
    StructField('eventTime', StringType(), True)])

    # Parse value from binay to string
    json_df = streaming_df.selectExpr("cast(value as string) as value")

    # Apply Schema to JSON value column and expand the value
    from pyspark.sql.functions import from_json

    json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

    # Validate Schema
    # json_expanded_df.show(10, False)
    # json_expanded_df.printSchema()

    # Lets explode the data as devices contains list/array of device reading
    from pyspark.sql.functions import explode, col

    exploded_df = json_expanded_df \
        .select("customerId", "eventId", "eventOffset", "eventPublisher", "eventTime", "data") \
        .withColumn("devices", explode("data.devices")) \
        .drop("data")

    # Check the schema of the exploded_df,  post a kafka message and change readStream to read 
    # exploded_df.printSchema()
    # exploded_df.show(truncate=False)

    # Flatten the exploded df
    flattened_df = exploded_df \
        .selectExpr("customerId", "eventId", "eventOffset", "eventPublisher", "cast(eventTime as timestamp) as eventTime", 
                    "devices.deviceId as deviceId", "devices.measure as measure", 
                    "devices.status as status", "devices.temperature as temperature") 

    # Check the schema of the flattened_df,  post a kafka message and change readStream to read 
    # flattened_df.printSchema()
    # flattened_df.show(truncate=False)

    # Aggregate the dataframes to find the average temparature
    # per Customer per device throughout the day for SUCCESS events
    from pyspark.sql.functions import to_date, avg

    agg_df = flattened_df.where("STATUS = 'SUCCESS'") \
        .withColumn("eventDate", to_date("eventTime", "yyyy-MM-dd")) \
        .groupBy("customerId","deviceId","eventDate") \
        .agg(avg("temperature").alias("avg_temp"))

    # Check the schema of the agg_df, post a kafka message and change readStream to read 
    # agg_df.printSchema()
    # agg_df.show()

    # Write the output to console sink to check the output
    writing_df = agg_df.writeStream \
        .format("console") \
        .option("checkpointLocation","checkpoint_dir") \
        .outputMode("complete") \
        .start()
        
    # Start the streaming application to run until the following happens
    # 1. Exception in the running program
    # 2. Manual Interruption
    writing_df.awaitTermination()