package cn.itcast.streaming

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaCumsumer {


  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = {
      val sparkconf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
     //   .registerKryoClasses(Array(ConsumerRecord[String, String]))


      new StreamingContext(sparkconf, Seconds(5))
    }

    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("node1.itcast.cn", 9092, storageLevel = StorageLevel.MEMORY_AND_DISK)


    //位置策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

    //kafkaParams参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1.itcast.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //消费者订阅信息
    val consumerStrategy: ConsumerStrategy[String,String] = ConsumerStrategies.Subscribe(
      Array("wc-topic"),
      kafkaParams
    )

    //1.通过新的API获取kafka topic数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategy,
      consumerStrategy
    )

    //获取kafka中topic [K,V]存在 获取Topic中Value数据:Message信息
    val inputDStream: DStream[String] = kafkaDStream.map(record => record.value())

    //依据业务需求,对每条数据进行处理
    val resultDStream: DStream[(String, Int)] = inputDStream
      .transform(rdd =>
        rdd
          .filter(line => null != line && line.trim.split("\\s+").length > 0)
          .flatMap(line => line.trim.split("\\s+"))
          .map(word => (word, 1))
          .reduceByKey((temp, item) => temp + item)
      )

    resultDStream.foreachRDD(
      (rdd,time)=>{
        val dateformat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        println("==============")
        println(s"dateTime: ${dateformat}")
        if(!rdd.isEmpty()) {
          rdd.foreachPartition(iter => iter.foreach(println))
        }
      }
    )


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true,stopGracefully = true)






  }

}
