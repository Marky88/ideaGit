package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用Spark实现词频统计WordCount程序
 */
object _00SparkWordCount {
	
	def main(args: Array[String]): Unit = {
		// TODO: 创建SparkContext实例对象，首先构建SparkConf实例，设置应用基本信息
		val sc: SparkContext = {
			SparkContext.getOrCreate(
				new SparkConf()
					.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    				.setMaster("local[2]")
			)
		}
		
		// TODO: 第一步、从HDFS读取文件数据，sc.textFile方法，将数据封装到RDD中
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")
		
		
		// TODO: 第二步、调用RDD中高阶函数，进行处理转换处理，函数：flapMap、map和reduceByKey
		/*
			过滤掉空数据
			按照分隔符分割单词
			转换单词为二元组，表示每个单词出现一次
			按照单词分组，对组内执进行聚合reduce操作，求和
		 */
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 分割单词，转换二元组，输出  - TODO： MapReduce框架中map方式实现功能
			.flatMap(line => {
				val words: Array[String] = line.trim.split("\\s+")
				val tuples: Array[(String, Int)] = words.map(word => (word, 1))
				// 返回
				tuples
			})
			// 按照单词分组聚合
			.reduceByKey(_ + _)
		
		
		// TODO: 第三步、将最终处理结果RDD保存到HDFS或打印控制台
		resultRDD.foreach(tuple => println(tuple))
		
		
		// 开始测试时，为例查看监控，线程休眠
		Thread.sleep(100000000)
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
