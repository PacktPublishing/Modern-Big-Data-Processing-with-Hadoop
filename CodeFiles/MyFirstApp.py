from pyspark.sql import SparkSession
# Path to the file in HDFS
csvFile = "employees.csv"
# Create a session for this application
spark = SparkSession.builder.appName("MyFirstApp").getOrCreate()
# Read the CSV File
csvTable = spark.read.format("csv").option("header",
"true").option("delimiter", "\t").load(csvFile)
# Print the total number of records in this file
print "Total records in the input : {}".format(csvTable.count())
# Stop the application
spark.stop()
