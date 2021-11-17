package cn.itcast.movie

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

//获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000
object TopMovie {

  def main(args: Array[String]): Unit = {

    val sparkRDD: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

    import sparkRDD.implicits._

    val ratingRDD: Dataset[String] = sparkRDD.read.textFile("datas/input/ml-1m/ratings.dat")
    /*    println(s"Count= ${ratingRDD.count()}")
    println(ratingRDD.first())*/


    val mdf: DataFrame = ratingRDD
      .filter(lines => null != lines && lines.trim.split("::").length == 4)
      .map(line => {
        val arr: Array[String] = line.trim.split("::")
        //(user_id, item_id, ranking.toDouble, times.toLong)
        (arr(0), arr(1), arr(2), arr(3))
      }
      )
      .toDF("user_id", "item_id", "rating", "times")

    /*    mdf.printSchema()
    mdf.show()*/


    /*    //SQL
    mdf.createOrReplaceTempView("topMovie")

    val moviesql: DataFrame = sparkRDD.sql(
      """
        |
        |select
        |   item_id,count(*) as numb,round(avg(rating),2) as avgScore
        | from topMovie
        | group by item_id having numb >2000
        | order by avgScore desc,numb desc limit 10
        |
        |""".stripMargin
    )
  moviesql.printSchema()
    moviesql.show()*/


    //DSL编程实现TOP10
    val resultDSL: Dataset[Row] = mdf
      .select($"item_id", $"rating")
      .groupBy($"item_id")
      .agg(
        count($"item_id").as("totalnumb"),
        round(avg($"rating"), 2).as("avgScore")
      )
     // .select($"item_id")   这行加上select 进行选择显示
      .filter($"totalnumb" > 2000)
      .orderBy($"avgScore".desc, $"totalnumb".desc)
      .limit(10)


    resultDSL.printSchema()
    resultDSL.show(10,truncate = false)


    println("===============================")

   // resultDSL.printSchema()
    // resultDSL.show()
   // resultDSL.select($"item_id", $"avgScore").show()

    //将分析结果的数据保存到外部存储的系统
    //开启缓存
    resultDSL.persist(StorageLevel.MEMORY_AND_DISK)

    /*    //保存到CSV文件中
    resultDSL
      .coalesce(1)
      .write
      .mode("overwrite")
      .csv("datas/output/top10-movies")*/


    /*    //保存到Mysql中
    resultDSL
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .jdbc(
        "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true", //
        "db_test.tb_top10_movies",
        new Properties()
      )*/

    /*  //mysql方式二
  val props = new Properties()
    props.put("driver","com.mysql.cj.jdbc.Driver")
    props.put("user","root")
    props.put("password","123456")
    resultDSL
      .coalesce(1)
      .write
      .mode("overwrite")
      .jdbc("jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      "db_test.tb_top10",
        props
      )*/

    //mysql方式三
    resultDSL
      .coalesce(1)
      .foreach(data => SaveMysql(data))

    val rdd: RDD[Row] = resultDSL.rdd
    resultDSL.printSchema()


  //关闭缓存 关闭SparkSession
    resultDSL.unpersist()
    sparkRDD.stop()
  }


  def  SaveMysql(datas: Row)= {

    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager.getConnection(
        "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "root",
        "123456"
      )

      val sql: String = "replace into db_test.top10_define (item_id,totalnumb,avgScore) values (?,?,?)"
      pstmt = conn.prepareStatement(sql)

      //开启事务
      val autoCommit: Boolean = conn.getAutoCommit()
      conn.setAutoCommit(false)

      pstmt.setString(1,datas.getString(0))
      pstmt.setLong(2,datas.getLong(1))
      pstmt.setDouble(3,datas.getDouble(2))
      //执行
      pstmt.execute()

      conn.commit()
      conn.setAutoCommit(autoCommit)

    } catch {
      case e :Throwable =>
                conn.rollback()
                throw e
       //Exception=>e.printStackTrace()
    } finally {
      if(null != pstmt){
        pstmt.close()
      }
      if(null != conn){
        conn.close()
      }

    }

  }




}
