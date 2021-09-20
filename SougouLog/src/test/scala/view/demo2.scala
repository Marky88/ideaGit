package view

import cn.itcast.Mysql.WriteMysql.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object demo2 {

  val sc :SparkContext ={
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    SparkContext.getOrCreate(conf)
  }

  def main(args: Array[String]): Unit = {

    val inputRDD: RDD[String] = sc.textFile("datas/input/wordcount.txt")

    val resultRDD: RDD[String] = inputRDD
      .filter(x => {
        println("filter===========")
        true
      }
      )
      .map(words => {
        println("map=========")
        words
      }
      )
      .flatMap(x => {
        println("flatMap==============")
        List(x)
        //println(s"-----------${x}")
      })

    println("+++++++++")

    println(s"Count=${resultRDD.count()}")
    println(s"count1=${resultRDD.map(println)}")


  }

}
