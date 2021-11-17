package start

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 使用SparkSQL进行词频统计WordCount：DSL
 */
object _03SparkDSLWordCount {
	
	def main(args: Array[String]): Unit = {
		
		// 使用建造设设计模式，创建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
			.getOrCreate()
		import spark.implicits._
		
		// TODO: 使用SparkSession加载数据
		val inputDS: Dataset[String] = spark.read.textFile("datas/wordcount.data")
		/*
			value: String
		 */
		
		// 编写DSL，调用DataFrame中方法（类似SQL关键词）
		val resultDF: Dataset[Row] = inputDS
			// 分割每行数据，转换为单词
			.select(
				// SQL: SELECT explode(split(trim(value), "\\s+")) AS word FROM tbl_lines
				explode(split(trim($"value"), "\\s+")).as("word")
			)
			// 按照单词分组，统计个数 -> SELECT word, COUNT(1) AS count FROM tmp GROUP word
			.groupBy($"word").count()
			// 按照词频降序排序
			.orderBy($"count".desc)
			// 获取前10条数据
			.limit(10)
	
		/*
		root
		 |-- word: string (nullable = true)
		 |-- count: long (nullable = false)
		
		+---------+-----+
		|word     |count|
		+---------+-----+
		|spark    |11   |
		|hive     |6    |
		|mapreduce|4    |
		|hadoop   |3    |
		|sql      |2    |
		|hdfs     |2    |
		+---------+-----+
		 */
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
		
		// 应用结束，关闭资源
		Thread.sleep(100000)
		spark.stop()
	}
	
}
