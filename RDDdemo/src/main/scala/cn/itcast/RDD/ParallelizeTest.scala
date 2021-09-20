package cn.itcast.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeTest {

  val sc : SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("parallize")
   SparkContext.getOrCreate(conf)
  }

  def main(args: Array[String]): Unit = {
    val lines: RDD[(String, String)] = sc.wholeTextFiles("datas/input/wordcount.txt", minPartitions = 2)
   // val lines: RDD[(String, String)] = sc.wholeTextFiles("hdfs://node1.itcast.cn:8020/datas/wordcount.data", minPartitions = 2)
      println(s"Partitions number ${lines.partitions.length}")
      println(lines.map(li=>li._1))
/*    val linesSeq: Seq[String] = Seq(
      "hadoop scala hive scala hadoop",
      "spark hadoop spark scala spark",
      "hive hadoop hdfs scala"
    )*/
    //并行化集合
   // val lines: RDD[String] = sc.parallelize(linesSeq, numSlices = 2)
    val resultRDD: RDD[(String, Int)] = lines.flatMap(line => line._2.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((temp, item) => temp + item)

    resultRDD.foreach(rd =>println(rd)+"  ")
    println(resultRDD.getNumPartitions)
    sc.stop()

  }

}
