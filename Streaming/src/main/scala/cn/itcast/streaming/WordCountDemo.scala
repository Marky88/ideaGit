package cn.itcast.streaming

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

//先开启hdfs zookeeper kafka
// 生产者开启:kafka-console-producer.sh --topic wc-topic --broker-list node1.itcast.cn:9092
object WordCountDemo {


  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = {
      val sparkconf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
      new StreamingContext(sparkconf, Seconds(5))
    }



   val  kafkaParams= Map[String, Object](
      "bootstrap.servers" -> "node1.itcast.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_id_1001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //位置策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferBrokers

    //消费者策略
     val  consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
     Array("wc-topic"),
       kafkaParams
     )


    //采用新的API 消费kafka的topic数据  使用KafkaUtils
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategy,
      consumerStrategy
    )

      //取出value的数据
    val inputStream: DStream[String] = kafkaStream.map(record => record.value())

    //使用transform  针对每次的rdd操作,更接近底层,性能更好 将一个DStream转换成另一个DStream
    val resultDS: DStream[(String, Int)] = inputStream.transform(rdd =>
      rdd.filter(lines => null != lines && lines.trim.length > 0)
        .flatMap(line => line.trim.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey((temp, item) => temp + item)
    )


    resultDS.foreachRDD((rdd,time)=>{
      val timeFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      println("=================")
      println(s"time: ${timeFormat.format(time.milliseconds)}")
      println("=================")

      if(!rdd.isEmpty()) {
        rdd
          .coalesce(1).foreachPartition(iter => iter.foreach(println))
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true,stopGracefully = true)
  }

}
