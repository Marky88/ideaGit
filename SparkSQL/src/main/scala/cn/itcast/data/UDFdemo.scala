package cn.itcast.data

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDFdemo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
/*      //显示指定集成Hive
      .enableHiveSupport()
      //设置Hive的连接地址
      .config("hive.metastore.uris","thrift://node1.itcast.cn:9083")
      .config("spark.sql.shuffle.partitions","4")*/
      .getOrCreate()

    import spark.implicits._


    val jsonDF: DataFrame = spark.read.json("datas/input/employees.json")
    jsonDF.printSchema()
    jsonDF.show(10,truncate = false)

/*
    //定义UDF函数
    spark.udf.register(
      "change_name",
      (name : String) => name.toUpperCase()
    )

    spark.udf.register(
      "trans_name",
      (name:String) => name.toLowerCase()
    )

    jsonDF.createOrReplaceTempView("view_temp_emp")

    spark.sql(
      "select change_name(name) as name1,salary from view_temp_emp"
    ).show(10,truncate = false)

    spark.sql(
      "select trans_name(name) as name2,salary from view_temp_emp"
    ).show(10,truncate = false)
    */

    println("===============================")
    //定义UDF函数在DSL中使用
    import org.apache.spark.sql.functions._

    val lowerUDF: UserDefinedFunction = udf(
      (name: String) => name.toLowerCase()
    )

    jsonDF.select(
     lowerUDF($"name").as("new_name"),
      $"salary"
    ).show()





  }
}
