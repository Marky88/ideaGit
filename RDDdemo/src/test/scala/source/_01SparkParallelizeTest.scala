package source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 采用并行化的方式构建Scala集合Seq中的数据为RDD
	 * - 将Scala集合转换为RDD
	 *      sc.parallelize(seq)
	 * - 将RDD转换为Scala中集合
		 * rdd.collect() -> Array
		 * rdd.collectAsMap() - Map,要求RDD数据类型为二元组
 */
object _01SparkParallelizeTest {
	
	def main(args: Array[String]): Unit = {
		
		val sc: SparkContext = {
			// sparkConf对象
			val sparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[2]")
			// sc 实例对象
			SparkContext.getOrCreate(sparkConf)
		}
		
		// TODO: 1、Scala中集合Seq序列存储数据
		val linesSeq: Seq[String] = Seq(
			"hadoop scala hive spark scala sql sql", //
			"hadoop scala spark hdfs hive spark", //
			"spark hdfs spark hdfs scala hive spark" //
		)
		
		// TODO: 2、并行化集合
		/*
		 def parallelize[T: ClassTag](
		      seq: Seq[T],
		      numSlices: Int = defaultParallelism
		 ): RDD[T]
		 */
	//	val inputRDD: RDD[String] = sc.parallelize(linesSeq, numSlices = 2)
		
		// TODO: 加载本地文件系统中数据
		/*
		def textFile(
	      path: String,
	      minPartitions: Int = defaultMinPartitions
	    ): RDD[String]
		 */
		val inputRDD: RDD[String] = sc.textFile(
			"datas/wordcount.data", //
			minPartitions = 2 //
		)
		
		// TODO: 3、词频统计
		val resultRDD = inputRDD
			.flatMap(line => line.split("\\s+"))
			.map(word => (word, 1))
			.reduceByKey((tmp, item) => tmp + item)
		
		// TODO: 4、输出结果
		resultRDD.foreach(println)
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
	
}
