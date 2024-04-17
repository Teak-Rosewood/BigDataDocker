from pyspark.sql import SparkSession
from pyspark.sql.functions import when, sum, avg, stddev, count, min, max

spark = SparkSession.builder.appName("name").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/workdir/data.csv")

df.printSchema()
df.show()
print("After Changing:")
df_changed = df.withColumn("Card_type", when(df["Card_type"] == "Checking", "Cash").otherwise(df["Card_type"]))

df_changed.show()

df_filtered = df_changed.drop("Date")

df_filtered.show()

df_final = df_filtered.filter((df_filtered["Amount"] > 30) & (df_filtered["Amount"] < 40))
df_final.show(1)
df_final = df_filtered.filter((df_filtered["Card_type"] == "Platinum Card") & (df_filtered["Amount"] < 200))
df_final = df_final.withColumnRenamed("Customer_NO", "id")
df_final.show()

amount = df_final.agg(sum("Amount"))
amount.show()
amount = df_final.agg(avg("Amount"))
amount.show()
amount = df_final.agg(stddev("Amount"))
amount.show()
amount = df_final.agg(count("Amount"))
amount.show()
amount = df_final.agg(min("Amount"))
amount.show()
amount = df_final.agg(max("Amount"))
amount.show()

df_final.write().csv("/output_asd", header=True)