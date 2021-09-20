package func.agg

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * Spark框架：RDD中聚合函数 -> reduce \ fold \ aggregate
 */
object _06SparkAggTest {
	
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
		
		// 1. 并行化方式，创建RDD
		val datasRDD: RDD[Int] = sc.parallelize((1 to 10).toList, numSlices = 2)
		
		// TODO: step1-将RDD中每个分区数据打印出来
		datasRDD.foreachPartition(iter => {
			// 每个分区数据被1个Task处理，每个Task处理分区数据时，有1个TaskContext对象
			val id = TaskContext.getPartitionId()
			println(s"p-${id}: ${iter.mkString(", ")}")
		})
		
		println(" ======================================= ")
		// TODO： step2-reduce函数，累加和
		val sumValue: Int = datasRDD.reduce((tmp, item) => {
			val id = TaskContext.getPartitionId()
			println(s"p-${id}: tmp = ${tmp}, item = ${item}, sum = ${tmp + item}")
			tmp + item
		})
		println(s"SumValue = ${sumValue}")
		
		
		println("========================================================")
		// TODO: step3-fold 函数，累计额和
		val foldValue: Int = datasRDD.fold(0)((tmp, item) => {
			val id = TaskContext.getPartitionId()
			println(s"p-${id}: tmp = ${tmp}, item = ${item}, sum = ${tmp + item}")
			tmp + item
		})
		println(s"foldValue = ${foldValue}")
		
		println("========================================================")
		// TODO： step4-aggregate函数，累计求和
		/*
			 def aggregate[U: ClassTag]
			 (zeroValue: U) // 初始化聚合中间临时变量的值，用户自己指定
			 (
			    seqOp: (U, T) => U, // 对分区内数据聚合的函数
			    combOp: (U, U) => U // 对分区间聚合结构数据进行聚合的聚合函数
			 ): U
		 */
		val aggValue: Int = datasRDD.aggregate(0)(
			// seqOp: (U, T) => U
			(tmp: Int, item: Int) => {
				println(s"seq: p-${TaskContext.getPartitionId()}: tmp = ${tmp}, item = ${item}, sum = ${tmp + item}")
				tmp + item
			},
			// combOp: (U, U) => U
			(tmp: Int, item: Int) => {
				println(s"comb: p-${TaskContext.getPartitionId()}: tmp = ${tmp}, item = ${item}, sum = ${tmp + item}")
				tmp + item
			}
		)
		println(s"aggValue = ${aggValue}")
		
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
