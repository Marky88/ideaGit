package cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中缓存函数，将数据缓存到内存或磁盘、释放缓存
 */
object _08SparkCacheTest {
	
	def main(args: Array[String]): Unit = {
		// 创建应用程序入口SparkContext实例对象
		val sc: SparkContext = {
			// 1.a 创建SparkConf对象，设置应用的配置信息
			val sparkConf: SparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// 1.b 传递SparkConf对象，构建Context实例
			new SparkContext(sparkConf)
		}
		
		// 读取文本文件数据
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data", minPartitions = 2)
		
		// 缓存数据: 将数据缓存至内存
		inputRDD.cache()
		
		// 使用Action函数触发缓存
		inputRDD.count()
		
		// 释放缓存
		inputRDD.unpersist()
		
		/*
			缓存数据：选择缓存级别
		*/
		inputRDD.persist(StorageLevel.MEMORY_AND_DISK)
		
		// 应用程序运行结束，关闭资源
		Thread.sleep(1000000)
		sc.stop()
	}
	
}
