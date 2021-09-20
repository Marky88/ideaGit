package cn.itcast.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Accmulartorwordcount {

  val list: List[String] = List(",", ".", "!", "#", "$", "%")

  val sc: SparkContext =
  {
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
  }


  def main(args: Array[String]): Unit = {
    val list: List[String] = List(",", ".", "!", "#", "$", "%")
    val inputRDD: RDD[String] = sc.textFile("datas/input/datas.input")

    val broadcastList: Broadcast[List[String]] = sc.broadcast(list)
    //定义累加器
    val accumulator: LongAccumulator = sc.longAccumulator("number_accum")

    //分隔单词 过滤数据
    val wordsRDD: RDD[String] = inputRDD
      .filter(lines => null != lines && lines.length > 0)   //filter中的结果是Boolean类型
      .flatMap(line => line.trim.split("\\s+"))
      .filter(word => {
        val listValue: List[String] = broadcastList.value
        val isFlag: Boolean = listValue.contains(word)
        if (isFlag) {
          accumulator.add(1L) //如果单词为符号,累加器进行加1
        }
        ! isFlag
      }
      )

    val output: RDD[(String, Int)] = wordsRDD.map(word => (word, 1)).reduceByKey((temp, item) => temp + item)
    output.foreach(println)
    println(s"过滤符合数据的个数:${accumulator.value}")

    sc.stop()
  }

}
