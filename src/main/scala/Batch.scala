import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{mean, _}

object Batch extends App {

  val spark = SparkSession
      .builder
      .appName("event-batch")
      .master("local")
      .getOrCreate()

  import spark.implicits._

  val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Data Engineer Take home dataset - dataset.csv")

  val df_structured = df.withColumn("timestamp_next",lead(col("timestamp"),1)
      .over(Window.partitionBy("user_id").orderBy("timestamp")))
      .withColumn("duration",
          when($"timestamp_next".isNotNull,unix_timestamp($"timestamp_next") - unix_timestamp($"timestamp"))
        .otherwise(15*60))
      .filter("event=='flyer_open' or event=='item_open'")
      .filter("duration<=900")
      .filter($"flyer_id" rlike("[0-9]+"))
      .filter($"merchant_id" rlike("[0-9]+"))
      .cache()

  val df_user = df_structured.groupBy($"user_id")
      .agg(mean("duration"))
      .write
      .option("header", "true")
      .csv("output/user")

  val df_flyer = df_structured.groupBy($"flyer_id", $"merchant_id")
    .agg(sum("duration"), count("event"), countDistinct("user_id"), mean("duration"))
    .write
    .option("header", "true")
    .csv("output/flyer")


}