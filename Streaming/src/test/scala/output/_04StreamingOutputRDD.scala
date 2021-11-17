package output

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object _04StreamingOutputRDD {
	
	def main(args: Array[String]): Unit = {
		
		// TODO: 1. 构建StreamingContext实例对象，传递时间间隔BatchInterval
		val ssc: StreamingContext = {
			// a. 创建SparkConf对象，设置应用基本信息
			val sparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]")
				// TODO：设置数据输出文件系统的算法版本为2
				.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
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
		val inputDStream: DStream[String] = ssc.socketTextStream(
			"node1.itcast.cn",
			9999,
			storageLevel = StorageLevel.MEMORY_AND_DISK
		)
		
		// TODO: 3. 依据业务需求，调用DStream中转换函数（类似RDD中转换函数）
		/*
			TODO: 能对RDD操作的就不要对DStream操作，当调用DStream中某个函数在RDD中也存在，使用针对RDD操作
			def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
		 */
		// 此处rdd就是DStream中每批次RDD数据
		val resultDStream: DStream[(String, Int)] = inputDStream.transform{ rdd =>
			val resultRDD: RDD[(String, Int)] = rdd
				.filter(line => null != line && line.trim.length > 0)
				.flatMap(line => line.trim.split("\\s+"))
				.map(word => (word, 1))
				.reduceByKey((tmp, item) => tmp + item)
			// 返回结果RDD
			resultRDD
		}
		
		// TODO: 4. 定义数据终端，将每批次结果数据进行输出
		//resultDStream.print()
		resultDStream.foreachRDD((rdd, time) => {  // 此处rdd属于每批次分析结果RDD
			// 转换每批次时间
			val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    				.format(time.milliseconds)
			
			println(s"-------------------------------------------\nBatchTime: ${batchTime}\n-------------------------------------------")
			
			// TODO： 判断结果RDD是否存在，如果存在，再输出
			if(! rdd.isEmpty()){
				rdd.foreachPartition(iter => iter.foreach(item => println(item)))
			}
		})
		
		
		// TODO: 5. 启动流式应用，等待终止
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
