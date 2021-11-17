package cn.itcast.baidu


import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}


//生产者的数据 ba2e1b8d1ed77c96,0.1.112.106,2021-06-22 20:34:28697,广电总局:支持查处阴阳合同等问题

//实时消费Kafka Topic数据，经过ETL（过滤、转换）后，保存至HDFS文件系统中，BatchInterval为：10s
object StreamingAppTest {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 5)
    val kafkaStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //消费者每次消费的是一个KV键值对,数据信息存在value中
    val etlDStream: DStream[String] = kafkaStream.transform(rdd => //rdd代表每一个批次的数据  对每行数据进行处理,每条数据是一个KV键值对,只使用V
      {
      val etlRDD: RDD[String] = rdd
        .filter(record => null != record && null != record.value() && record.value().trim.split(",").length == 4)
        .mapPartitions { iter =>
          // 创建DbSearcher对象
          val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), "datas/input/ip2region.db")
          iter.map(records => {
            val message: String = records.value().trim
            // TODO: 解析IP地址
            // 传递IP地址，解析获取数据
            val block: DataBlock = dbSearcher.btreeSearch(message.split(",")(1))
            // 获取解析省份和城市
            val region: String = block.getRegion
            val Array(_, _, province, city, _) = region.split("\\|")

            // 将省份和城市追加到原来数据上
            s"$message,$province,$city"
          }
          )
        }
          etlRDD
      }
    )

    // 4. 保存数据至HDFS文件系统
    etlDStream.foreachRDD( (rdd,time) => {
      val timeformat: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
      val batchTime: String = timeformat.format(time.milliseconds)
      if(!rdd.isEmpty()) {
        rdd.coalesce(1).saveAsTextFile(s"datas/output/etl/search-logs-$batchTime")

        }
      rdd.foreachPartition(iter=>iter.foreach(println))
    }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true,stopGracefully = true)

  }

}
