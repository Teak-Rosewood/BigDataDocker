import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("name").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/workdir/data_1.csv")

df.printSchema()
df.show()
println("After Changing:")
val df_changed = df.withColumn("Card_type", when(df("Card_type") === "Checking", "Cash").otherwise(df("Card_type")))

df_changed.show()

val df_filtered = df_changed.drop("Date")

df_filtered.show()

val df_final1 = df_filtered.filter($"Amount" > 30 && $"Amount" < 40)
df_final1.show(1)
val df_final2 = df_filtered.filter($"Card_type" === "Platinum Card" && $"Amount" < 200)
val df_final = df_final2.withColumnRenamed("Customer_NO", "id")
df_final.show()

val amountSum = df_final.agg(sum("Amount"))
amountSum.show()
val amountAvg = df_final.agg(avg("Amount"))
amountAvg.show()
val amountStddev = df_final.agg(stddev("Amount"))
amountStddev.show()
val amountCount = df_final.agg(count("Amount"))
amountCount.show()
val amountMin = df_final.agg(min("Amount"))
amountMin.show()
val amountMax = df_final.agg(max("Amount"))
amountMax.show()

df_final.write.option("header", "true").csv("/output")