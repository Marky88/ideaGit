package convert

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 自定义Schema方式转换RDD为DataFrame
 */
object _05SparkRDDSchema {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象，设置应用名称和master
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			.getOrCreate()
		import spark.implicits._
		
		// 1. 加载电影评分数据，封装数据结构RDD
		val ratingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data")
		
		// 2. TODO: step1. RDD中数据类型为Row：RDD[Row]
		val rowRDD: RDD[Row] = ratingRDD
			.filter(line => null != line && line.trim.split("\\t").length == 4)
			.map{line =>
				val Array(userId, itemId, rating, timestamp) = line.trim.split("\\t")
				// 封装到Row对象
				Row(userId, itemId, rating.toDouble, timestamp.toLong)
			}
		
		// 3. TODO：step2. 针对Row中数据定义Schema：StructType
		val schema: StructType = StructType(
			StructField("user_id", StringType, nullable = true) ::
			StructField("item_id", StringType, nullable = true) ::
			StructField("rating", DoubleType, nullable = true) ::
			StructField("timestamp", LongType, nullable = true) :: Nil
		)
		
		// 4. TODO：step3. 使用SparkSession中方法将定义的Schema应用到RDD[Row]上
		val dataframe: DataFrame = spark.createDataFrame(rowRDD, schema)
		
		dataframe.printSchema()
		dataframe.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
