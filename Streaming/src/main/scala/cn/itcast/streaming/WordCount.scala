package cn.itcast.streaming


import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {


  def main(args: Array[String]): Unit = {


  val ssc: StreamingContext = {
    val sparkConf = new SparkConf()
    sparkConf
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[3]")

  //最少3个 一个receiver以task运行,另外两个task 并行运行任务
    new StreamingContext(sparkConf, Seconds(5))
  }

    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream(
      "node1.itcast.cn",
      9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK)
    //val inputStream2: ReceiverInputDStream[String] = ssc.socketTextStream("node1.itcast.cn", 9998)
 //   val inputStream: DStream[String] = inputStream1.union(inputStream2)

    //transform针对每一批次RDD操作
    val resultRDD: DStream[(String, Int)] = inputStream.transform(rdd => {
      val resultdd: RDD[(String, Int)] = rdd
        .flatMap(line => line.trim.split("\\s+"))
        .map(word => word -> 1)
        .reduceByKey((temp, item) => temp + item)
      resultdd
    }
    )

    resultRDD.foreachRDD( (rdd,time) =>{
      //使用FastDateFormat 转换时间格式
      val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
      println("==============================")
      println(s"Batch Time ${batchTime}")
      println("==============================")

      if (!batchTime.isEmpty){
        //保存数据,降低分区,开启缓存
        val cacheRDD: RDD[(String, Int)] = rdd.coalesce(1).cache()
        cacheRDD.foreachPartition(iter => iter.foreach(println))
        //保存到本地
        cacheRDD.saveAsTextFile(s"datas/output/stream-wc-${time.milliseconds}")
        cacheRDD.unpersist()
        }
     }

    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true,stopGracefully = true)


  }

}
