package cn.itcast.test

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 将业务数据封装到DataFrame中，依据Schema信息，在ClickHouse数据库中创建表和删除，并且将数据集DataFrame保存到表中，此外实现更新和删除操作。
 */
object ClickHouseSparkDemo {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置属性信息
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
    		.config("spark.sql.shuffle.partitions", "2")
    		.getOrCreate()
		import spark.implicits._
		
		// 2. 加载JSON格式数据
		val orderDF: DataFrame = spark.read.json("datas/order.json")
		/*
			root
			 |-- areaName: string (nullable = true)
			 |-- category: string (nullable = true)
			 |-- id: long (nullable = true)
			 |-- money: string (nullable = true)
			 |-- timestamp: string (nullable = true)
		 */
		//orderDF.printSchema()
		/*
			+--------+--------+---+-----+--------------------+
			|areaName|category|id |money|timestamp           |
			+--------+--------+---+-----+--------------------+
			|北京    |平板电脑|1  |1450 |2019-05-08T01:03.00Z|
			|北京    |手机    |2  |1450 |2019-05-08T01:01.00Z|
			|北京    |手机    |3  |8412 |2019-05-08T01:03.00Z|
			|上海    |电脑    |4  |1513 |2019-05-08T05:01.00Z|
			|北京    |家电    |5  |1550 |2019-05-08T01:03.00Z|
		 */
		//orderDF.show(10, truncate = false)
		
		// 3. 依据DataFrame数据集，在ClickHouse数据库中创建表和删除表
		// TODO: 创建表，先构建DDL语句
		val createDdl: String = ClickHouseUtils.createTableDdl("test", "tbl_order", orderDF.schema)
		//println(createDdl)
		//ClickHouseUtils.executeUpdate(createDdl)
		// TODO: 删除表，先构建DDL语句，再执行
		val dropDdl: String = ClickHouseUtils.dropTableDdl("test", "tbl_order")
		//ClickHouseUtils.executeUpdate(dropDdl)
		
		// 4. 保存DataFrame数据集到ClickHouse表中
		val insertSQL = ClickHouseUtils.createInsertSQL("test", "tbl_order", orderDF.columns)
		//println(insertSQL)
		//ClickHouseUtils.insertData(orderDF, "test", "tbl_order")
		
		// 5. 更新数据到ClickHouse表中
		val updateDF: DataFrame = Seq(
			(15, 9999, "2020-12-08T01:03.00Z"),
			(16, 9999, "2020-12-08T01:03.00Z")
		).toDF("id", "money", "timestamp")
		ClickHouseUtils.updateData(updateDF, "test", "tbl_order")
		
		
		// 6. 删除ClickHouse表中数据
		val deleteDF: DataFrame = Seq( Tuple1(1), Tuple1(2), Tuple1(3)).toDF("id")
		ClickHouseUtils.deleteData(deleteDF, "test", "tbl_order")
		
		// 应用结束，关闭资源
		spark.stop
	}
	
}
