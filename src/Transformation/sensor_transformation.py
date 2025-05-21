import json
from postgres_Connection import postgres_conn

#Transformation

from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("PostgreSQL Connection with PySpark")
        .getOrCreate()
    )

    print("Spark session created successfully")
    return spark
    
def transform_data_load_table(filepath):
    with open(filepath, "r") as f:
        content = f.read()
        data = json.loads(content)
   
    #print(data)  # This will be a Python list
    
    conn = postgres_conn()
    cur= conn.cursor()
    
    #Insert data into table
    for item in data:
        record = json.loads(item)  # parse each string in the list
        print(record['timestamp'],record['sensor_id'], record['value'],record['city'], record['country'])
        cur.execute(
            "INSERT INTO SensorInfo (timestamp,sensor_id,value,city,country) VALUES (TO_TIMESTAMP(%s), %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (record['timestamp'],record['sensor_id'], record['value'],record['city'], record['country'])
        )
    
    #Commit and close
    conn.commit()
    cur.close()
    conn.close()
    
    print("Data inserted successfully.")

def spark_transform_data_load_table(filepath,spark_session):
    conn = postgres_conn()
    cur= conn.cursor()

    #Commit and close
    conn.commit()
    cur.close()
    conn.close()
    
    print("Data inserted successfully.")

   
if __name__ == '__main__':
    filepath="de_e2e/data/kafka_messages.json"
    spark=create_spark_session()
    spark_transform_data_load_table(filepath,spark)
    transform_data_load_table(spark)

    # Stop the Spark session when done
    spark.stop()