package convert

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 采用反射的方式将RDD转换为DataFrame
 */
object _04SparkRDDInferring {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象，设置应用名称和master
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[3]")
    		.getOrCreate()
		import spark.implicits._
		
		// 1. 加载电影评分数据，封装数据结构RDD
		val ratingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data", minPartitions = 2)
		
		// 2. 将RDD数据类型转化为 MovieRating
		/*
			采用自动类型推断，将RDD转换为DataFrame，此时RDD中数据类型必须时CaseClass
			TODO：解析RDD中字符串，将其数据封装到CaseClass实例对象即可
						196	242	3	881250949
								|
						MovieRating()
		 */
		val rdd: RDD[MovieRating] = ratingRDD
			.filter(line => null != line && line.trim.split("\\t").length == 4)
			.map{line =>
				//val array: Array[String] = line.trim.split("\\t")
				val Array(userId, itemId, rating, timestamp) = line.trim.split("\\t")
				// 构建样例类对象
				MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
			}
		
		// 3. 通过隐式转换，直接将CaseClass类型RDD转换为DataFrame
		val dataframe: DataFrame = rdd.toDF()
		
		dataframe.printSchema()
		dataframe.show(10, truncate = false)

		// 应用结束，关闭资源
		spark.stop()
	}
	
}
