package cn.itcast.data

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示RDD中基本函数使用
 */
object _03SparkBasicTest {
	
	def main(args: Array[String]): Unit = {
		// 创建SparkContext实例对象，传递SparkConf对象，设置应用配置信息
		val sc: SparkContext = {
			// a. 创建SparkConf对象
			val sparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// b. 传递sparkConf对象，构建SparkContext实例
			SparkContext.getOrCreate(sparkConf)
		}
		
		// step1. 读取数据
		val inputRDD: RDD[String] = sc.textFile("file:///E:\\Project\\Spark\\datas\\input\\wordcount.txt", minPartitions = 2)
		
		// step2. 处理数据
		val filterRDD: RDD[String] = inputRDD
			// 过滤数据，将空白数据过滤掉
			.filter(line => null != line && line.trim.length > 0)
			/*// 分割单词
			.flatMap(line => line.trim.split("\\s+"))
			// 转换为二元组
			.map(word => (word ,1))
			// 分组聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// step3. 输出数据
		resultRDD.foreach(item => println(item))*/
	//	filterRDD.foreachPartition(iter=> iter.foreach(println))
		filterRDD.foreach(println)
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
