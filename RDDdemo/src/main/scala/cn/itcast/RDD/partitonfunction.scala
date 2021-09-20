package cn.itcast.RDD


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object partitonfunction {

  val sc : SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("parallize")
    SparkContext.getOrCreate(conf)
  }


  def main(args: Array[String]): Unit = {


    val datasRDD: RDD[String] = sc.textFile("datas/input/wordcount.txt", minPartitions = 2)
    val etlRDD: RDD[String] = datasRDD.repartition(3)
    println(etlRDD.getNumPartitions)

    val resultRDD: RDD[(String, Int)] = etlRDD.filter(line => (null != line) && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .mapPartitions(it => it.map(li => (li, 1)))
      .reduceByKey((a, b) => a + b)

    resultRDD.coalesce(1).foreachPartition(wd=>wd.foreach(w =>println(w)))

    sc.stop()
  }

}
