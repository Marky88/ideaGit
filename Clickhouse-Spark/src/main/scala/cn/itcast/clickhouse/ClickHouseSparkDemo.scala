package cn.itcast.clickhouse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @Description
 * @Author Marky
 * @Date 2021/7/9 19:47
 */
object ClickHouseSparkDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions","2")
      .getOrCreate()
    import spark.implicits._


    val orderDF: DataFrame = spark.read.json("datas/input/order.json")
/*    root
    |-- areaName: string (nullable = true)
    |-- category: string (nullable = true)
    |-- id: long (nullable = true)
    |-- money: string (nullable = true)
    |-- timestamp: string (nullable = true)*/
    /*
         * +--------+--------+---+-----+--------------------+
    |areaName|category|id |money|timestamp           |
    +--------+--------+---+-----+--------------------+
      |北京    |平板电脑|1  |1450 |2019-05-08T01:03.00Z|
      |北京    |手机    |2  |1450 |2019-05-08T01:01.00Z|
      |北京    |手机    |3  |8412 |2019-05-08T01:03.00Z|
      |上海    |电脑    |4  |1513 |2019-05-08T05:01.00Z|
      |北京    |家电    |5  |1550 |2019-05-08T01:03.00Z|
     */



    // 3. 依据DataFrame数据集，在ClickHouse数据库中创建表和删除表
    val createDdl: String = ClickHouseUtils.createTableDdl("test", "tbl_order", orderDF)
   // println(createDdl)
    //执行语句创建表
    //ClickHouseUtils.executeUpdate(createDdl)
    //执行语句删除表
    val dropDdl: String = ClickHouseUtils.dropTableDdl("test", "tbl_order")
   // ClickHouseUtils.executeUpdate(dropDdl)

    // 4.保存DataFrame数据集到ClickHouse表中
    //ClickHouseUtils.insertData(orderDF,"test","tbl_order")


    // 5. 更新数据到ClickHouse表中
    val updateDF: DataFrame = Seq(
      (3, 9999, "2020-12-08T01:03.00Z"),
      (4, 9999, "2020-12-08T01:03.00Z")
    ).toDF("id","money","timestamp")

  ClickHouseUtils.updateData(orderDF,"test","tbl_order","id")




    // 6. 删除ClickHouse表中数据
    val deleteDF: DataFrame = Seq(Tuple1(1),Tuple1(2),Tuple1(3)).toDF("id")

    //7.关闭连接
    spark.stop()









  }

}
