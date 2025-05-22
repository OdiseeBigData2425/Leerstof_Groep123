# in streaming context werk ik vaak met aparte files
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

# Create a local StreamingContext with two working thread and batch interval of 5 second
sc = SparkContext("local[2]", "networkwordcount")
sc.setLogLevel("ERROR") # reduce spam of logging
ssc = StreamingContext(sc, 5) # 5 om hoeveel seconden wordt er nieuwe data verwerkt
ssc.checkpoint("checkpoint")

lines = ssc.socketTextStream('localhost', 19999)

words = lines.flatMap(lambda line: line.split(' '))
pairs = words.map(lambda word: (word, datetime.now()))
#wordCounts = pairs.reduceByKey(lambda x,y: x+y)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        return datetime.now()
    if newValues is None:
        return runningCount
    return max(newValues)

runningCounts = pairs.updateStateByKey(updateFunction)

runningCounts.pprint() # can niet collect zijn want er is geen globaal resultaat

ssc.start()
ssc.awaitTermination()
