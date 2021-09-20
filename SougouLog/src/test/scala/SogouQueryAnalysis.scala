import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
	 * 1. 搜索关键词统计，使用HanLP中文分词
	 * 2. 用户搜索次数统计
	 * 3. 搜索时间段统计
 * 数据格式：
 *      访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 **/
object SogouQueryAnalysis {
	
	def main(args: Array[String]): Unit = {
		// step1、构建SparkSession实例对象
		val spark: SparkSession = getSparkSession(this.getClass)
		import spark.implicits._
		
		// step2、加载数据，封装到RDD
		val rawRDD: RDD[String] = spark.sparkContext.textFile("datas/input/SogouQ.reduced", minPartitions = 3)
	
		// step3、ETL转换，将RDD数据转换以后，封装为DataFrame/Dataset
		val dataframe: DataFrame = process(rawRDD, spark)
		//dataframe.show(10,truncate = false)
		
		// step4、指标分析（三个业务需求指标统计计算）
		report(dataframe)
		
		// step5、应用结束，关闭资源
		spark.stop()
	}
	
	/**
	 * 创建SparkSession实例对象，运行在本地模式，基本参数设置
	 */
	def getSparkSession(clazz: Class[_]): SparkSession = {
		SparkSession.builder()
    		.appName(clazz.getSimpleName.stripSuffix("$"))
    		.master("local[3]")
    		.config("spark.sql.shuffle.partitions", "3")
			// 设置序列化Kryo
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    		.getOrCreate()
	}
	
	/**
	 * 对日志数据RDD进行转换，封装到DataFrame数据集中，方便后续处理
	 */
	def process(rdd: RDD[String], spark: SparkSession): DataFrame = {
		// 导入隐式转换
		import spark.implicits._
		
		/*
			RDD 转换DataFrame三种方式：
				RDD[CaseClass]、自定义Schema、toDF指定名称[RDD中数据类型为元组]
		 */
		val etlDF: DataFrame = rdd
			// 过滤数据
			.filter(line => null != line && line.trim.split("\\s+").length == 6)
			// 解析封装
    		.map{line =>
			    val array: Array[String] = line.trim.split("\\s+")
			    // 封装至元组
			    (
				    array(0), array(1), //
				    array(2).replaceAll("\\[", "").replace("]", ""), //
				    array(3).toInt, array(4).toInt, array(5) //
			    )
		    }
			// 转换为DataFrame
    		.toDF("query_time", "user_id", "query_words", "result_rank", "click_rank", "click_url")
		
		// 返回转换后DataFrame
		etlDF
	}
	
	/**
	 * 对DataFrame数据按照业务需求进行指标统计分析
	 *      需求一、搜索关键词统计，使用HanLP中文分词
	 *      需求二、用户搜索次数统计
	 *      需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
	 */
	def report(dataframe: DataFrame): Unit = {
		
		// TODO: 多个业务分析，缓存数据
		dataframe.persist(StorageLevel.MEMORY_AND_DISK)
		
		// 需求一、搜索关键词统计，使用HanLP中文分词
		// reportSearchKeywords(dataframe)
		reportSearchKeywordsSQL(dataframe)
		
		// 需求二、用户搜索次数统计
		// reportSearch(dataframe)
		
		// 需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
		// reportHour(dataframe)
		
		// 数据不在使用，释放资源
		dataframe.unpersist()
	}
	
	/**
	 * 业务分析指标一：搜索关键词统计，使用HanLP中文分词
	 */
	def reportSearchKeywords(dataframe: DataFrame): Unit = {
		import dataframe.sparkSession.implicits._
		
		// 自定义UDF函数，进行中文分词
		import org.apache.spark.sql.functions.udf
		val to_hanlp: UserDefinedFunction = udf(
			(queryWords: String) => {
				val terms: util.List[Term] = HanLP.segment(queryWords.trim)
				// 导入隐式转，将Java中列表转换为Scala中列表
				import scala.collection.JavaConverters._
				terms.asScala.map(term => term.word).toArray
			}
		)
		
		val reportDF: DataFrame = dataframe
			// 将搜索关键词分割为单词
    		.select(
			    explode(to_hanlp($"query_words")).as("query_word")
		    )
			// 按照搜索关键词分组统计
    		.groupBy($"query_word").count()
			// 降序排序
    		.orderBy($"count".desc)
    		.limit(10)
		reportDF.printSchema()
		reportDF.show(10, truncate = false)
	}
	
	def reportSearchKeywordsSQL(dataframe: DataFrame): Unit = {
		val spark: SparkSession = dataframe.sparkSession
		
		// 第一步、注册DataFrame为临时视图
		dataframe.createOrReplaceTempView("tmp_view_sougo")
		
		// TODO: 定义UDF函数，在SQL中使用
		spark.udf.register(
			"to_hanlp", //
			(search: String) => {
				val terms: util.List[Term] = HanLP.segment(search.trim)
				// 导入隐式转，将Java中列表转换为Scala中列表
				import scala.collection.JavaConverters._
				terms.asScala.map(term => term.word).toArray
			}
		)
		
		// 第二步、编写SQL并执行
		val resultDF: DataFrame = spark.sql(
			"""
			  |WITH tmp AS (
			  |  SELECT explode(to_hanlp(query_words)) AS query_word FROM tmp_view_sougo
			  |)
			  |SELECT query_word, COUNT(1) AS total FROM tmp GROUP BY query_word ORDER BY total DESC LIMIT 10
			  |""".stripMargin)
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
	}
	
}
