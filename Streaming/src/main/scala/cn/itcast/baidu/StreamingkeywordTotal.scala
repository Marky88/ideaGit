package cn.itcast.baidu


import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

//bb58cd4111d6d090,0.4.128.81,2021-06-22 21:51:37074,吴尊友提醒五一不参加大型聚集聚会
//实时累计统计各个搜索词的出现的个数
object StreamingkeywordTotal {

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)
    val kafkaStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //设置检查点 理解为作为中间变量  累加时需要将上一次的值记录到这里,下一次出来的值与这个进行累加作为新的累加值
    ssc.checkpoint("datas/output/chectpoint")

    //消费者每次消费的是一个KV键值对,数据信息存在value中
    val resultRDD: DStream[(String, Int)] = kafkaStream.transform(rdd => //rdd代表每一个批次的数据  对每行数据进行处理,每条数据是一个KV键值对,只使用V
    {
      val etlRDD: RDD[(String, Int)] = rdd
        .filter(record => null != record && null != record.value() && record.value().trim.split(",").length == 4)
        .mapPartitions { iter =>
          iter.map(
            records => (records.value().trim.split(",")(3), 1)
          )
        }
        .reduceByKey(_ + _)
      etlRDD
      }
    )

  //  resultRDD.foreachRDD(rdd => rdd.foreachPartition(keyword =>keyword.foreach(println)))

    // 3. TODO: step2. 将当前批次聚合结果与以前状态数据进行聚合操作（状态更新）
 /*   def updateStateByKey[S: ClassTag](
           updateFunc: (Seq[V], Option[S]) => Option[S]
            ): DStream[(K, S)] =*/
    val stateDStream: DStream[(String, Int)] = resultRDD.updateStateByKey(
      (values: Seq[Int], state: Option[Int]) => {
        //获取之前的状态
        val prevoiusState: Int = state.getOrElse(0)  //state.getOrElse(0)的含义是，如果该单词没有历史词频统计汇总结果， 那么，就取值为0，如果有历史词频统计结果，就取历史结果。
        //获取当前批次的状态
        val currentState: Int = values.sum
        //合并
        val latestState = prevoiusState + currentState
        //返回最后的值
        Some(latestState)
      }
    )

    stateDStream.foreachRDD((rdd,time)=> {  //有时间戳???
      val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
      val format: String = dateFormat.format(time.milliseconds)
      println(s"======${format.format(time.milliseconds)}======")
      if (!rdd.isEmpty()){
        rdd.coalesce(1).foreachPartition(iter=>iter.foreach(println))
      }
    }
      )


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true,stopGracefully = true)

  }

}
