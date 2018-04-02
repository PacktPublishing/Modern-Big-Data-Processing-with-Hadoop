from pyspark import SparkContext
from pyspark.streaming import StreamingContext
context = SparkContext(appName="StreamingDedup")
stream = StreamingContext(context, 5)
records = stream.socketTextStream("localhost", 5000)
records.map(lambda record: (record, 1)).reduceByKey(lambda x,y: x + y).pprint()
ssc.start()
ssc.awaitTermination()
