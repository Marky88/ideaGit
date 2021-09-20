package ckpt

import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD数据Checkpoint设置，案例演示
 */
object _09SparkCkptTest {
	
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
		
		// TODO: 设置检查点目录，将RDD数据保存到那个目录
		sc.setCheckpointDir("datas/ckpt-rdd")
		
		// 读取文件数据
		val datasRDD = sc.textFile("datas/wordcount.data")
		
		// TODO: 调用checkpoint函数，将RDD进行备份，需要RDD中Action函数触发
		datasRDD.checkpoint()
		datasRDD.count() // Checkpoint属于lazy执行，需要action函数触发
		
		// TODO: 再次执行count函数, 此时从checkpoint读取数据
		println(s"Count = ${datasRDD.count()}")
		
		// 应用程序运行结束，关闭资源
		Thread.sleep(1000000000)
		sc.stop()
	}
	
}
