package func.iter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分区操作函数：mapPartitions和foreachPartition
 */
object _04SparkIterTest {
	
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
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data", minPartitions = 2)
		
		// step2. 处理数据
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 过滤数据
			.filter(line => line.trim.length != 0 )
			// 对每行数据进行单词分割
			.flatMap(line => line.trim.split("\\s+"))
			// 转换为二元组
    		//.map(word => word -> 1)
			/*
			  def mapPartitions[U: ClassTag](
			      f: Iterator[T] => Iterator[U],
			      preservesPartitioning: Boolean = false
			 ): RDD[U]
			 */
    		.mapPartitions(iter => iter.map(word => (word, 1)))
			// 分组聚合
			.reduceByKey((tmp, item) => tmp + item)
		
		// step3. 输出数据
		//resultRDD.foreach(item => println(item))
		/*
			def foreachPartition(f: Iterator[T] => Unit): Unit
		 */
		resultRDD.foreachPartition(iter => iter.foreach(item => println(item)))
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
