package view

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _07SparkViewTest {
	
	
	def main(args: Array[String]): Unit = {
		
		// 1. 在Spark 应用程序中，入口为：SparkContext，必须创建实例对象，加载数据和调度程序执行
		val sc: SparkContext = {
			// 创建SparkConf对象，设置应用相关信息，比如名称和master
			val sparkConf: SparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[1]")
			// 构建SparkContext实例对象，传递SparkConf
			new SparkContext(sparkConf)
		}
		
		val inputRDD: RDD[String] = sc.textFile("datas/11.data")
		
		val resultRDD: RDD[String] = inputRDD
			.filter(x => {
				println("filter..............")
				true
			})
			.map(x => {
				println("map..................")
				x
			})
			.flatMap(x => {
				println("flatmap...................")
				List(x)
			})
		/*
			filter..............
			filter..............
			filter..............
			map..................
			map..................
			map..................
			flatmap...................
			flatmap...................
			flatmap...................
			
			filter..............
			map..................
			flatmap...................
			filter..............
			map..................
			flatmap...................
			filter..............
			map..................
			flatmap...................
		 */
		
		println(s"Count = ${resultRDD.count()}")
		
		// 5. 当应用运行结束以后，关闭资源
		Thread.sleep(10000000)
		sc.stop()
	}
	
}
