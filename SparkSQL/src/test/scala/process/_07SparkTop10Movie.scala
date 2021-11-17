package process

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 需求：对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
	 * 第一步、读取电影评分数据，从本地文件系统读取
	 * 第二步、转换数据，指定Schema信息，封装到DataFrame
	 * 第三步、基于SQL方式分析
	 * 第四步、基于DSL方式分析
 */
object _07SparkTop10Movie {
	
	def main(args: Array[String]): Unit = {
		// step0. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[3]")
			// TODO: 设置SparkSQL Shuffle时，分区数目，合理性设置
    		.config("spark.sql.shuffle.partitions", "3")
    		.getOrCreate()
		import spark.implicits._
		
		// TODO: step1. 读取电影评分数据，从本地文件系统读取，封装数据至RDD中
		val ratingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-1m/ratings.dat", minPartitions = 3)
		//println(s"Count = ${ratingRDD.count()}")
		//println(s"${ratingRDD.first()}")
		
		// TODO: step2. 将RDD数据转换为DataFrame，使用toDF指定列名称转换，此时要求RDD数据类型为元组
		/*
			a. 解析每条数据
			b. 构建元组对象
			c. 调用toDF函数，指定列名称
		 */
		val ratingDF: DataFrame = ratingRDD
    		.filter(line => null != line && line.trim.split("::").length == 4)
			// 解析数据，构建元组对象
    		.map{line =>
			    // a. 解析每条数据
			    val array = line.trim.split("::")
			    // b. 构建元组对象
			    (array(0), array(1), array(2).toDouble, array(3).toLong)
		    }
			// 调用toDF函数，指定列名称
    		.toDF("user_id", "movie_id", "rating", "timestamp")
//		ratingDF.printSchema()
//		ratingDF.show(10, truncate = false)
//		println(s"Count = ${ratingDF.count()}")
		
		// TODO: step3. 基于SQL方式分析
		/*
			a. 注册为临时视图
			b. 编写SQL，执行分析
		 */
		// a. 将DataFrame注册为临时视图
		ratingDF.createOrReplaceTempView("tmp_view_rating")
		// b. 编写SQL，执行分析
		val top10MoviesDF: DataFrame = spark.sql(
			"""
			  |SELECT
			  |  movie_id, COUNT(1) AS rating_total,
			  |  ROUND(AVG(rating), 2) AS rating_avg
			  |FROM
			  |  tmp_view_rating
			  |GROUP BY
			  |  movie_id
			  |HAVING
			  |  rating_total >= 2000
			  |ORDER BY
			  |  rating_avg DESC, rating_total DESC
			  |LIMIT 10
			  |""".stripMargin)
		//top10MoviesDF.printSchema()
		//top10MoviesDF.show(10, truncate = false)
		
		// TODO: step3. 使用DSL方式分析数据
		/*
			获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
		 */
		val resultDF: Dataset[Row] = ratingDF
			// a. 按照电影ID分组，聚合计算评分次数和平均评分
			.groupBy($"movie_id")
			.agg(
				round(avg($"rating"), 2).as("avg_rating"), //
				count($"movie_id").as("cnt_rating") //
			)
			// b. 评分次数大于2000
			.where($"cnt_rating" > 2000)
			// c. 先按照评分降序排序，再按照次数降序排序
			.orderBy($"avg_rating".desc, $"cnt_rating".desc)
			// d. 前10条数据
			.limit(10)
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
		
		
		// TODO: step 4. 将分析结果数据保存到外部存储系统中，比如保存到MySQL数据库表中或者CSV文件中
		
		// 4.1 保存结果数据至MySQL表中
		

		// 4.2 保存结果数据至CSv文件中
		
		
		Thread.sleep(10000000)
		// step5. 应用结束，关闭资源
		spark.stop()
	}
	
}
