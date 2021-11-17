package todf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 隐式调用toDF函数，将数据类型为元组的Seq和RDD集合转换为DataFrame
 */
object _06SparkSQLToDF {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象，设置应用名称和master
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[3]")
    		.getOrCreate()
		import spark.implicits._
		
		// 1. 将数据类型为元组的RDD，转换为DataFrame
		val rdd: RDD[(Int, String, String)] = spark.sparkContext.parallelize(
			List((1001, "zhangsan", "male"), (1003, "lisi", "male"), (1003, "xiaohong", "female"))
		)
		// 调用toDF方法，指定列名称，将RDD转换为DataFrame
		val df: DataFrame = rdd.toDF("id", "name", "gender")
		df.printSchema()
		df.show(10, truncate = false)
		
		println("==========================================================")
		
		// 定义一个Seq序列，其中数据类型为元组
		val seq: Seq[(Int, String, String)] = Seq(
			(1001, "zhangsan", "male"), (1003, "lisi", "male"), (1003, "xiaohong", "female")
		)
		// 将数据类型为元组Seq序列转换为DataFrame
		val dataframe: DataFrame = seq.toDF("id", "name", "gender")
		dataframe.printSchema()
		dataframe.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
