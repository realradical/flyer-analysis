import org.apache.spark.sql.SparkSession

object DebugReadData extends App {
  val spark = SparkSession
    .builder
    .appName("event-batch")
    .master("local")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("output")

  df.orderBy("user_id", "start_time").show(1000, false)
}
