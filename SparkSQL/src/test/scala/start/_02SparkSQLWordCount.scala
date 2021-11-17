package start

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SparkSQL进行词频统计WordCount：SQL
 */
object _02SparkSQLWordCount {
	
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
		root
            |-- value: string (nullable = true)

		+--------------------+
		|               value|
		+--------------------+
		|hadoop spark hado...|
		|mapreduce spark s...|
		|hive spark hadoop...|
		|spark hive sql sq...|
		|hdfs hdfs mapredu...|
		+--------------------+
		 */
		
		/*
			table: words , column: value
					SQL: SELECT value, COUNT(1) AS count  FROM words GROUP BY value
		 */
		// step 1. 将Dataset或DataFrame注册为临时视图
		inputDS.createOrReplaceTempView("tbl_lines")
		
		// step 2. 编写SQL并执行
		val resultDF: DataFrame = spark.sql(
			"""
			  |SELECT
			  |  t.word, COUNT(1) AS total
			  |FROM (
			  |  SELECT explode(split(trim(value), "\\s+")) AS word FROM tbl_lines
			  |) t
			  |GROUP BY t.word
			  |ORDER BY total DESC
			  |LIMIT 10
			  |""".stripMargin)
		/*
		root
		 |-- word: string (nullable = true)
		 |-- total: long (nullable = false)
		
		+---------+-----+
		|word     |total|
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
		resultDF.show(50, truncate = false)
		
		// 应用结束，关闭资源
		Thread.sleep(100000)
		spark.stop()
	}
	
}
