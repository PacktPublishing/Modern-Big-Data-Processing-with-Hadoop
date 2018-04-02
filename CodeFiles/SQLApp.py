from pyspark.sql import SparkSession
# Path to the file in HDFS
csvFile = "employees.csv"
# Create a session for this application
spark = SparkSession.builder.appName("SQLApp").getOrCreate()
# Read the CSV File
csvTable = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(csvFile)
csvTable.show(3)
# Create a temporary view
csvView = csvTable.createOrReplaceTempView("employees")
# Find the total salary of employees and print the highest salary makers
highPay = spark.sql("SELECT first_name, last_name, emp_no, SUM(salary) AS total FROM employees GROUP BY emp_no, first_name, last_name ORDER BY SUM(salary)")
# Generate list of records
results = highPay.rdd.map(lambda rec: "Total: {}, Emp No: {}, Full Name: {} {}".format(rec.total, rec.emp_no, rec.first_name, rec.last_name)).collect()
# Show the top 5 of them
for r in results[:5]:
    print(r)
# Stop the application
spark.stop()
