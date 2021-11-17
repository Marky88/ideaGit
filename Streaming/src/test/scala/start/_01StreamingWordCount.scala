package start

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object _01StreamingWordCount {
	
	def main(args: Array[String]): Unit = {
		
		// TODO: 1. 构建StreamingContext实例对象，传递时间间隔BatchInterval
		val ssc: StreamingContext = {
			// step1、创建SparkConf对象，设置属性信息
			val sparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]")
			// step2、传递batchInterval时间间隔，构建流式上下文镀锡
			new StreamingContext(sparkConf, Seconds(5))
		}
		
		// TODO: 2. 定义数据源，获取流式数据，封装到DStream中
		/*
		  def socketTextStream(
		      hostname: String,
		      port: Int,
		      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
		    ): ReceiverInputDStream[String]
		    一行一行读取Socket中数据
		 */
		val inputDStream: DStream[String] = ssc.socketTextStream("node1.itcast.cn", 9999)
		
		
		// TODO: 3. 依据业务需求，调用DStream中转换函数（类似RDD中转换函数）
		/*
				spark hive hive spark spark hadoop
						|
				spark、hive、hive、spark、spark、hadoop
						|
				(spark, 1)、(hive, 1)、(hive, 1)、(spark, 1)、(spark, 1)、(hadoop, 1)
						|
				(spark, list[1,1,1])、(hive, list[1,1])、(haodop, list[1])
							(spark, 1+1+1=3) 、(hive, 1+1=2)、(hadoop, 1)
		 */
		val resultDStream: DStream[(String, Int)] = inputDStream
			// 按照分隔符分割单词
			.flatMap(line => line.split("\\s+"))
			// 转换单词为二元组，表示每个单词出现一次
			.map(word => word -> 1)
			// 按照单词分组，对组内执进行聚合reduce操作，求和
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 4. 定义数据终端，将每批次结果数据进行输出
		resultDStream.print()
		
		// TODO: 5. 启动流式应用，等待终止
		ssc.start()
		// 当流式应用启动以后，一直运行，除非认为终止或程序异常终止，否则一直等待关闭
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
