import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.read.option("header", "true").csv("/src/data.csv")
    df.show()
  }
}