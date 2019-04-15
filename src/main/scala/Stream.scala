import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, TimestampType}

case class InputRow(user_id: String,
                    timestamp: java.sql.Timestamp,
                    event: String,
                    flyer_id: String,
                    merchant_id: String
                   )

case class EventState(user_id: String,
                      var lastEntry: InputRow
                      )

case class EventOutput(user_id: String,
                       var start_time: java.sql.Timestamp,
                       var end_time: java.sql.Timestamp,
                       var duration: Double,
                       var flyer_id: String,
                       var merchant_id: String,
                       var event: String
                   )

object Stream {

  val BOOTSTRAPSERVERS = "127.0.0.1:9092"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("event-stream")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    //Read raw data from Kafka topic
    val fileStreamDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAPSERVERS)
      .option("startingOffsets", "latest")
      .option("subscribe", "user-event")
      .load()

    //Cast value column to string and reconstruct the dataframe
    val df=fileStreamDf.select(col("value").cast("string"))

    val df_structured = df.selectExpr(
      "split(value,',')[0] as timestamp"
      ,"split(value,',')[1] as user_id"
      ,"split(value,',')[2] as event"
      ,"split(value,',')[3] as flyer_id"
      ,"split(value,',')[3] as merchant_id")
      .select(col("timestamp").cast(TimestampType),
        col("user_id").cast(StringType),
        col("event").cast(StringType),
        col("flyer_id").cast(StringType),
        col("merchant_id").cast(StringType)
      )

    def updateStateWithEvent(state:EventState, input:InputRow):EventState = {
      state.lastEntry = input

      state
    }

    def updateAcrossEvents(user_id:String,
                           inputs: Iterator[InputRow],
                           oldState: GroupState[EventState]):Iterator[EventOutput] = {

      var state:EventState = if (oldState.exists) oldState.get else EventState(user_id, null)

      var eventList  = List[EventOutput]()

      for (input <- inputs) {
        if (state.lastEntry != null) {
          val eventOutput: EventOutput = EventOutput(user_id,
            state.lastEntry.timestamp,
            input.timestamp,
            (input.timestamp.getTime - state.lastEntry.timestamp.getTime)/1000.0,
            state.lastEntry.flyer_id,
            state.lastEntry.merchant_id,
            state.lastEntry.event
          )

          eventList ::= eventOutput
        }
        state = updateStateWithEvent(state, input)
        oldState.update(state)
      }

      eventList.toIterator

    }

    val consoleOutput = df_structured.as[InputRow]
      .groupByKey(_.user_id)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(updateAcrossEvents)
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .outputMode("update")
      .option("checkpointLocation", "checkpoint/")
      .option("truncate", "false")
      .format("csv")
      .start("output/")
      .awaitTermination()
  }
}
