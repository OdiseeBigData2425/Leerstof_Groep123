from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.master('local').appName('test').getOrCreate()

lines = spark.readStream.format('socket').option('host', 'localhost').option('port', 19999).load() 
# readStream ipv read

words = lines.select(explode(split(lines.value, ' ')).alias('word'))
wordCounts = words.groupBy('word').count()

app = wordCounts.writeStream.outputMode('complete').format('console')
#writeStream ipv write

app.start().awaitTermination()
