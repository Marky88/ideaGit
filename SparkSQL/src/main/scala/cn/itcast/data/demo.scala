package cn.itcast.data

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

object demo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

/*    //首行是列名称
    val csvDF: DataFrame = spark.read
      .option("sep", "\\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("datas/input/ml-100k/u.dat")
    csvDF.printSchema()
    csvDF.show(10,truncate = false)*/

    //首行不是列名称
/*

    val styp: StructType = new StructType()
      .add("user_id", IntegerType, nullable = true)
      .add("movie_id", IntegerType, nullable = true)
      .add("rating", DoubleType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    val csvDF: DataFrame = spark.read
      .schema(styp)
      .option("sep","\\t")
     // .option("sep","\\s")
      .csv("datas/input/ml-100k/u.data")
    csvDF.printSchema()
    csvDF.show(10,truncate = false)
*/

    //1.读取mysql数据库 简写版本 表格固定只能读一个表
    val props = new Properties()
    props.put("driver","com.mysql.cj.jdbc.Driver")
    props.put("user","root")
    props.put("password","123456")

    //加载配置文件的另一种写法
    val pro: Properties = new Properties() {
      {
        put("driver", "com.mysql.cj.jdbc.Driver")
        put("user", "root")
        put("password", "123456")
      }
    }
/*
    val mysqlDF: DataFrame = spark.read.jdbc(
      "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      "db_test.movie",
      props
    )

    mysqlDF.printSchema()
    mysqlDF.show(10,truncate = false)
    */


  //2.多表的关联
    val table: String =
      "(select  d.deptname, e.hiredate,e.sal from db_test.dept d left join db_test.emp e on d.deptno = e.deptno) as temp"
    val mysqlDF: DataFrame = spark.read.jdbc(
      "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      //"(select  d.deptname, e.hiredate,e.sal from db_test.dept d left join db_test.emp e on d.deptno = e.deptno) as temp" ,
      table,
      props
    )

    mysqlDF.printSchema()
    mysqlDF.show(10,truncate = false)




    // 应用结束，关闭资源
    spark.stop()
  }

}
