package cn.itcast.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.immutable

/**
 * @author: Marky
 * @date: 2021/11/9 22:05
 * @description:
 */
object clickhouseDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()


    val inputDS = spark.read.json("datas/input/data/order.json")
    val df = inputDS.toDF()
    df.show(5)

    println("df:"+df.schema)
    println("name:"+df.schema.map(s => s.name))
    println("col"+df.columns)
    println(df.columns.toList)

    insertMysql(df)


    def insertMysql(df:DataFrame)={
      var schema = df.schema
      var colName: Seq[String] = df.columns.toList
      val names: String = colName.mkString(",")
      println("colName:"+names)
      val nums = colName.map(_ => "?").mkString(",")
      println("问号:"+ nums)


      df.coalesce(4).foreachPartition {iter =>
        var conn:Connection = null
        var pstmt: PreparedStatement = null

        try {
          Class.forName("com.mysql.cj.jdbc.Driver")
          conn = DriverManager.getConnection("jdbc:mysql://192.168.88.161:3306/Demo","root","123456")
          val sql = s"insert into order3 (${names}) values (${nums})"
          pstmt = conn.prepareStatement(sql)

          //每行数据处理
          iter.foreach{ row =>
            colName.foreach{column =>
              val index = schema.fieldIndex(column)
              val value = row.get(index)
              pstmt.setObject(index + 1 ,value)
            }

            pstmt.addBatch()
            var  count = 0
            if (count > 300){
              pstmt.executeBatch()
              count = 0
            }
          }

          pstmt.executeBatch()

        } catch {
          case e => e.printStackTrace()
        } finally {
          if (null != pstmt ) pstmt.close()
          if (null != conn ) conn.close()
        }

      }

    }










  }

}
