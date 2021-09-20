package cn.itcast.Mysql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

object WriteMysql {

    val sc :SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      SparkContext.getOrCreate(sparkConf)
    }

  def main(args: Array[String]): Unit = {

    val inputRDD: RDD[String] = sc.textFile("datas/input/wordcount.txt")

    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(lines => null != lines && lines.length > 0)
      .flatMap(words => words.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((temp, item) => temp + item)

   // resultRDD.foreach(println)

    resultRDD
      .coalesce(1)
      .foreachPartition(iter=> saveMysql(iter))

  }

  //将每个分区的数据保存到mysql数据库的表中,
  //datas表示迭代器,封装RDD中每个分区的数据
  def saveMysql(iter:Iterator[(String,Int)]) ={

    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager
        .getConnection("jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true","root","123456")
      val sql: String = "replace into db_test.tb_wordcount (word,count) values(?,?)"
       pstmt= conn.prepareStatement(sql)

      //考虑事务性 一个分区数据要么全部保存,要么都不保存
      val autoCommit: Boolean = conn.getAutoCommit
      //开启事务
      conn.setAutoCommit(false)
      iter.foreach{case (word,count) =>
        pstmt.setString(1,word)
        pstmt.setInt(2,count)
        //pstmt.execute()
        pstmt.addBatch()
      }
      pstmt.executeBatch()
    //  val c = 1/0

      //手动提交事务
      conn.commit()
      //还原数据库原来事务
      conn.setAutoCommit(autoCommit)

    } catch {
      case e :Throwable => {
        conn.rollback()   //自己增加 事务回滚
        throw e
      }

    }
    finally{
      if(null != conn){
        conn.close()
      }
      if(null != pstmt){
        pstmt.close()
      }
    }

  }

}
