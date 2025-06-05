from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col
import time
 
print('create spark')
 
spark = SparkSession.builder.appName("BookConsumer").getOrCreate()
 
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 19999).load()
lines.printSchema()
 
json_schema = StructType().add("timestamp", StringType()).add("line", StringType())
 
parsed = lines.withColumn("json", from_json(col("value"), json_schema)).select("json.*")
print("DataFrame schema:")
parsed.printSchema()
 
query = parsed.writeStream.outputMode('complete').format('console').start()
query.awaitTermination()