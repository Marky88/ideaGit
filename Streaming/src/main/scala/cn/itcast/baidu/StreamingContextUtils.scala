package cn.itcast.baidu

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy, PerPartitionConfig}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingContextUtils {

  /**
   * 获取StreamingContext实例，传递批处理时间间隔
   *
   * @param batchInterval 批处理时间间隔，单位为秒
   */
  def getStreamingContext(clazz:Class[_],batchInterval:Int): StreamingContext={
      val sparkconf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
        //设置kryo序列化
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))
        //设置保存文件数据时,算法版本:2
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))
        //创建流水上下文对象,传递sparkConf 和时间间隔
        new StreamingContext(sparkconf,Seconds(batchInterval))
    }


  /**
   * 从指定的Kafka Topic中消费数据，默认从最新偏移量（largest）开始消费
   * @param ssc StreamingContext实例对象
   * @param topicName 消费Kafka中Topic名称
   */

  def consumerKafka(ssc:StreamingContext,topicName:String):InputDStream[ConsumerRecord[String, String]]={

    val topics = Array(topicName)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1.itcast.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "gui_0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //3.根据创建时的消费者提供位置策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferBrokers

    //2.根据创建的消费者, 消费者开启订阅模式
    val consumerStrategy: ConsumerStrategy[String,String] = ConsumerStrategies.Subscribe(
      topics,
      kafkaParams
    )
    //1.通过新的API,创建消费者
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategy,
      consumerStrategy
    )
    //返回消费者策略
    kafkaDStream
  }




}
