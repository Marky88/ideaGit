package work

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object _02StreamingWordCountUnion {
	
	def main(args: Array[String]): Unit = {
		
		// TODO: 1. 构建StreamingContext实例对象，传递时间间隔BatchInterval
		val ssc: StreamingContext = {
			// a. 创建SparkConf对象，设置应用基本信息
			val sparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]")
			// b. 创建实例对象，设置BatchInterval
			new StreamingContext(sparkConf, Seconds(5))
		}
		
		
		// TODO: 2. 定义数据源，获取流式数据，封装到DStream中
		/*
		  def socketTextStream(
		      hostname: String,
		      port: Int,
		      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
		    ): ReceiverInputDStream[String]
		 */
		// netcat 端口号：9999
		val inputDStream01 = ssc.socketTextStream(
			"node1.itcast.cn", 9999, storageLevel = StorageLevel.MEMORY_AND_DISK
		)
		
		// netcat 端口号：8888
		val inputDStream02 = ssc.socketTextStream(
			"node1.itcast.cn", 8888, storageLevel = StorageLevel.MEMORY_AND_DISK)
		
		// 将多个流DStream进行合并
		val inputDStream: DStream[String] = inputDStream01.union(inputDStream02)
		
		// TODO: 3. 依据业务需求，调用DStream中转换函数（类似RDD中转换函数）
		val resultDStream = inputDStream
			.filter(line => null != line && line.trim.length > 0)
			.flatMap(line => line.trim.split("\\s+"))
			.map(word => (word, 1))
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 4. 定义数据终端，将每批次结果数据进行输出
		resultDStream.print()
		
		// TODO: 5. 启动流式应用，等待终止
		ssc.start()  // 启动Receivers接收器
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
