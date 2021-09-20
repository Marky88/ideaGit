package start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用Spark实现词频统计WordCount程序
 */
object _00SparkWordCount {
	
	def main(args: Array[String]): Unit = {
		// TODO: 创建SparkContext实例对象，首先构建SparkConf实例，设置应用基本信息
		val sc: SparkContext = {
			// 其一、构建SparkConf对象，设置应用名称和master
			val sparkConf: SparkConf = new SparkConf()
    			.setAppName("SparkWordCount")
    			.setMaster("local[2]")
			// 其二、创建SparkContext实例，传递sparkConf对象
			SparkContext.getOrCreate(sparkConf)
		}
		
		
		// TODO: 第一步、从HDFS读取文件数据，sc.textFile方法，将数据封装到RDD中
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")
		
		// TODO: 第二步、调用RDD中高阶函数，进行处理转换处理，函数：flapMap、map和reduceByKey
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 按照分隔符分割单词
			.flatMap(line => line.split("\\s+"))
			// 转换单词为二元组，表示每个单词出现一次
			.map(word => word -> 1)
			// 按照单词分组，对组内执进行聚合reduce操作，求和
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 第三步、将最终处理结果RDD保存到HDFS或打印控制台
		resultRDD.foreach(tuple => println(tuple))
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
