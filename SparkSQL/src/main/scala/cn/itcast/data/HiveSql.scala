package cn.itcast.data

import org.apache.spark.sql.{DataFrame, SparkSession}


object HiveSql {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //显示指定集成Hive
      .enableHiveSupport()
      //设置Hive的连接地址
      .config("hive.metastore.uris","thrift://node1.itcast.cn:9083")
      .config("spark.sql.shuffle.partitions","4")
      .getOrCreate()

    import spark.implicits._

    //方式1 加载table表
    val hiveDF: DataFrame = spark.read.table("db_hive.emp")

  //  hiveDF.printSchema()
   // hiveDF.show(10 ,truncate = false)


    //方式2 通过sql查询
    val hiveSql: DataFrame = spark.sql("select * from db_hive.emp")
    hiveSql.printSchema()
    hiveSql.show(10,truncate = false)


  }

}
