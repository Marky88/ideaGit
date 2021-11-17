package cn.itcast.baidu

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}


/**
 * 实时消费Kafka Topic数据，每隔一段时间统计最近搜索日志中搜索词次数
 * 批处理时间间隔：BatchInterval = 2s
 * 窗口大小间隔：WindowInterval = 4s
 * 滑动大小间隔：SliderInterval = 2s
 */
object StreamingWindow {


  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 2)
    //创建消费者
    val kafkaStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //设置窗口 大小4s 滑动2s
    val windowDS: DStream[ConsumerRecord[String, String]] = kafkaStream.window(
      Seconds(4),
      Seconds(2)
    )

    //统计关键词的次数
    val resultDS: DStream[(String, Int)] = windowDS.transform(rdd => {  //此处rdd就是一个窗口的数据
      val records: RDD[String] = rdd.map(record => record.value())
      val totaRDD: RDD[(String, Int)] = records.filter(record => null != record && record.trim.split(",").length == 4)
        .map(words => words.trim.split(",")(3) -> 1)
        //对每一个窗口进行统计
        .reduceByKey(_ + _)
      totaRDD
      }
    )

    resultDS.foreachRDD((rdd,time)=> {
      val dateFormat: String = FastDateFormat.getInstance("yyyyMMddHHmmssSSS").format(time.milliseconds)
        println(s"===========$dateFormat==========")

      if(!rdd.isEmpty()){
        rdd.coalesce(1).foreachPartition(iter =>iter.foreach(println))
      }
    }
    )


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true,stopGracefully = true)

  }


}
