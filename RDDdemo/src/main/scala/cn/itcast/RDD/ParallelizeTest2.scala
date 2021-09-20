package cn.itcast.RDD

import cn.itcast.RDD.ParallelizeTest.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object ParallelizeTest2 {

  val sc : SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("parallize")
    SparkContext.getOrCreate(conf)
  }

  def main(args: Array[String]): Unit = {
    val inputRDD: RDD[String] = sc.textFile("datas/input/wordcount.txt", minPartitions = 2)
/*    val word: RDD[(String, Int)] = inputRDD
                                    .flatMap(li => li.trim.split("\\s+"))
                                    .map(word => (word, 1))
                                    .reduceByKey((temp, item) => temp + item)  //二元组，按照key分组 value进行聚合
    word.foreach(wd =>println(wd))*/
/*
    val words: RDD[(String, Int)] = inputRDD.flatMap(li => li.trim.split("\\s+"))
      .mapPartitions { it => it.map(wd => (wd, 1)) }
      .reduceByKey((a, b) => a + b)

    words.foreachPartition{
      datas =>
        val id: Int = TaskContext.getPartitionId()

      datas.foreach{
        case(word,count) =>println(s"p-$id: word:$word, count:$count")
      }
    }*/

    inputRDD.cache()
    inputRDD.persist()

    //使用Action函数触发缓存
    println(s"count=${inputRDD.count()}")


  //释放缓存
    inputRDD.unpersist()

    //设置缓存级别
    inputRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"count:${inputRDD.count()}")

    sc.stop()

    }

}
