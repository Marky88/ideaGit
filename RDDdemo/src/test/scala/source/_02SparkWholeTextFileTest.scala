package source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 采用SparkContext#wholeTextFiles()方法读取小文件
 */
object _02SparkWholeTextFileTest {
	
	def main(args: Array[String]): Unit = {
		val sc: SparkContext = {
			// sparkConf对象
			val sparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// sc 实例对象
			SparkContext.getOrCreate(sparkConf)
		}
		
		/*
		  def wholeTextFiles(
		      path: String, // 目录
		      minPartitions: Int = defaultMinPartitions // 设置RDD分区数目
		  ): RDD[(String, String)]
		  TODO：将每个文件数据读取到RDD的1条记录中，返回类型为KeyValue对
		    Key：每个文件的路径，Value：表示每个文件内容
		 */
		val inputRDD: RDD[(String, String)] = sc.wholeTextFiles(
			"datas/ratings100", // 指定小文件路径
			minPartitions = 2 //
		)
		println("Partitions = " + inputRDD.getNumPartitions)
		
		println(s"Count = ${inputRDD.count()}")
		
		println("first： \n" + inputRDD.first())
		
		// 应用结束，关闭资源
		sc.stop()
		
	}
	
}
