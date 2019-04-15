import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object KafkaProducer {
  val BOOTSTRAPSERVERS = "127.0.0.1:9092"

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", BOOTSTRAPSERVERS)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val bufferedSource = Source.fromFile("src/main/resources/stream_data.csv")

    for (line <- bufferedSource.getLines.drop(1)) {
      Thread.sleep(50)
      producer.send(new ProducerRecord[String, String]("user-event", line))
    }

    producer.close()
  }
}
