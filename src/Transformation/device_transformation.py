from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import col

import os
os.environ["HADOOP_HOME"] = "C:/hadoop/hadoop-3.3.5"

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Transform data with PySpark")
        .config("spark.streaming.stopGracefullyOnShutdown", True) 
        .master("local[*]") 
        .getOrCreate()
    )

    print("Spark session created successfully")
    return spark

    
def spark_transform_data_load_table(filepath,spark_session):
    import os
    print(os.getcwd()) 

    # To allow automatic schemaInference while reading
    spark.conf.set("spark.sql.streaming.schemaInference", True)
    
    # Create the streaming_df to read from input directory
    streaming_df = (
        spark
        .readStream
        .format("json")
        .load(filepath)
    )

    # To the schema of the data, place a sample json file and change readStream to read 
    streaming_df.printSchema()

    # Lets explode the data as devices contains list/array of device reading    
    exploded_df = streaming_df.withColumn("data_devices", explode("data.devices"))
    
    # Check the schema of the exploded_df, place a sample json file and change readStream to read 
    exploded_df.printSchema()

    # Flatten the exploded df    
    flattened_df = (
        exploded_df
        .drop("data")
        .withColumn("deviceId", col("data_devices.deviceId"))
        .withColumn("measure", col("data_devices.measure"))
        .withColumn("status", col("data_devices.status"))
        .withColumn("temperature", col("data_devices.temperature"))
        .drop("data_devices")
    )
    # Check the schema of the flattened_df, place a sample json file and change readStream to read 
    flattened_df.printSchema()
    
    # Write the output to console sink to check the output
    flattened_df.writeStream \
        .format("csv") \
        .option("path", "data/device_files/output/") \
        .option("checkpointLocation", "output/stream_parquet/checkpoints/") \
        .outputMode("append") \
        .start()
    
    print('write completed')
    
if __name__ == '__main__':
    filepath="data/device_files/input/device_data.json"
    spark=create_spark_session()
    spark_transform_data_load_table(filepath,spark)
    #transform_data_load_table(filepath)

    # Stop the Spark session when done
    spark.stop()