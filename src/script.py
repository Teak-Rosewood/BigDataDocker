from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("./src/data.csv", header=True, inferSchema=True)
df.show()