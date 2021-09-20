package cn.itcast.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Logdemo {
  val sc : SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("parallize")
    SparkContext.getOrCreate(conf)

  }
/*  def main(args: Array[String]): Unit = {
  sc.parallelize()

  }*/


}
