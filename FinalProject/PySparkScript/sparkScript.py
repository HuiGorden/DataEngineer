from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# debug EMR
# EMR 6.8.0/spark 3.3.0, scala 2.12.15
# pyspark --jars /usr/lib/hudi/hudi-spark-bundle.jar --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog"  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0

# Global variable 
BOOTSTRAP_SERVERS='b-3.finalprojectmsk.fe0wsv.c14.kafka.us-east-1.amazonaws.com:9092,b-1.finalprojectmsk.fe0wsv.c14.kafka.us-east-1.amazonaws.com:9092,b-2.finalprojectmsk.fe0wsv.c14.kafka.us-east-1.amazonaws.com:9092'
table_name = 'bus_status'
checkpoint_location = "s3://hui-final-project/msk/checkpoint/sparkjob"


def write_batch(batch_df, batch_id):
    batch_df.write.format("org.apache.hudi") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.precombine.field", "event_time") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "routeId") \
    .option("hoodie.datasource.write.hive_style_partitioning", "true") \
    .option("hoodie.datasource.hive_sync.database", "final_project") \
    .option("hoodie.datasource.hive_sync.enable", "true") \
    .option("hoodie.datasource.hive_sync.table", "bus_status") \
    .option("hoodie.table.name", "bus_status") \
    .option("hoodie.datasource.hive_sync.partition_fields", "routeId") \
    .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor") \
    .option("hoodie.upsert.shuffle.parallelism", "100") \
    .option("hoodie.insert.shuffle.parallelism", "100")\
    .mode("append") \
    .save("s3://hui-final-project/hudi/output")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    # load schema from S3 
    schema = spark.read.json('s3://hui-final-project/bus_status_schema.json').schema

    # connect to AWS MSK
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("subscribe", "dbserver1.demo.bus_status").option("startingOffsets", "latest").load()

    transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

    transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("final-project").foreachBatch(write_batch).start().awaitTermination()