package cn.itcast.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reducefunction {

  val sc : SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("parallize")
    SparkContext.getOrCreate(conf)
  }

  def main(args: Array[String]): Unit = {
    val empRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhangliu"))
    )
    val deptRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "sales"), (1002, "tech"))
    )

    val joinRDD: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)
    println(joinRDD.collectAsMap())



  }

}
