package app.window

import app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时消费Kafka Topic数据，每隔一段时间统计最近搜索日志中搜索词次数
	 * 批处理时间间隔：BatchInterval = 2s
	 * 窗口大小间隔：WindowInterval = 4s
	 * 滑动大小间隔：SliderInterval = 2s
 */
object _08StreamingWindow {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 创建StreamingContext实例对象
		val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 2)
		// TODO: 设置检查点目录
		ssc.checkpoint(s"datas/spark/ckpt-${System.nanoTime()}")
		
		// 2. 从Kafka消费数据，采用New Consumer API
		val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")
		
		// TODO: 设置窗口：大小为4秒，滑动为2秒
		/*
			def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
		 */
		val windowDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.window(Seconds(4), Seconds(2))
		
		// 3. 对窗口中数据进行聚合统计
		val resultDStream: DStream[(String, Int)] = windowDStream.transform{ rdd => // TODO: 此处RDD就是窗口中所有批次数据组成的RDD
			// 数据格式：a1e44ac71488fccd,121.76.107.140,20210621154830895,外籍女子拒戴口罩冲乘客竖中指
			val reduceRDD: RDD[(String, Int)] = rdd
				// 过滤数据
				.filter(record => {
					null != record && null != record.value() && record.value().trim.split(",").length == 4
				})
				// 提取搜索关键词
				.map(record => {
					record.value().trim.split(",")(3) -> 1  // 二元组，表示此关键词出现1词
				})
				// 按照搜索词分组和聚合统计
				.reduceByKey(_ + _)
			// 返回当前批次聚合RDD结果
			reduceRDD
		}
		
		// 4. 将每批次结果数据进行输出
		resultDStream.foreachRDD((rdd, time) => {
			val format: FastDateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
			println("-------------------------------------------")
			println(s"Time: ${format.format(time.milliseconds)}")
			println("-------------------------------------------")
			// 判断每批次结果RDD是否有数据，如果有数据，再进行输出
			if(!rdd.isEmpty()){
				rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
			}
		})
		
		// 启动流式应用，等待终止结束
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
