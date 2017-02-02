package exercise

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// schema for sensor data
case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

object Sensor {
  // function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val rec = str.split(",")
    Sensor(rec(0), rec(1), rec(2), rec(3).toDouble, rec(4).toDouble, rec(5).toDouble, rec(6).toDouble, rec(7).toDouble, rec(8).toDouble)
  }
}

object SensorStreamConsumer extends Serializable {

  val timeout = 20 // Terminate after N seconds
  val batchSeconds = 2 // Size of batch intervals

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException("You must specify the topic, for example /user/user01/pump:sensor ")
    }


    val topics: String = args(0)
    System.out.println("Subscribed to : " + topics)

    val brokers = "maprdemo:9092"
    // not needed for MapR Streams, needed for Kafka
    val groupId = "testgroup"
    val offsetReset = "earliest"
    val batchInterval = "2"
    val pollTimeout = "1000"

    val sparkConf = new SparkConf().setAppName("SensorStream")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    /**
      * More information on available params at
      * https://kafka.apache.org/documentation/#consumerconfigs
      */
    val kafkaParams = {
      import ConsumerConfig._
      val StringDeserializerClass =
        "org.apache.kafka.common.serialization.StringDeserializer"
      Map[String, String](
        BOOTSTRAP_SERVERS_CONFIG -> brokers,
        GROUP_ID_CONFIG -> groupId,
        KEY_DESERIALIZER_CLASS_CONFIG -> StringDeserializerClass,
        VALUE_DESERIALIZER_CLASS_CONFIG -> StringDeserializerClass,
        AUTO_OFFSET_RESET_CONFIG -> offsetReset,
        ENABLE_AUTO_COMMIT_CONFIG -> "false",
        "spark.kafka.poll.time" -> pollTimeout
      )
    }

    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val sensorDStream = messages.map(_._2).map(Sensor.parseSensor)

    sensorDStream.foreachRDD { rdd =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        val sensorDF = rdd.toDF()
        // Display the top 20 rows of DataFrame
        println("sensor data")
        sensorDF.show()

        // apply SQL queries on the dataframe
        sensorDF.registerTempTable("sensor")
        val res = sqlContext.sql("SELECT resid, date, count(resid) as total FROM sensor GROUP BY resid, date")
        println("sensor count ")
        res.show
        val res2 = sqlContext.sql("SELECT resid, date, avg(psi) as avgpsi FROM sensor GROUP BY resid,date")
        println("sensor psi average")
        res2.show
      }
    }
    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}