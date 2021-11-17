package cn.itcast.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object WordCountDSL {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val inputRDD: Dataset[String] = spark.read.textFile("datas/input/wordcount.txt")

  println(inputRDD)
    println("++++++++++++++++++++++++")

    inputRDD.printSchema()
    inputRDD.show(12,truncate = false)

    println("=================")

    val result: Dataset[Row] = inputRDD
      .select(
        explode(split(trim($"value"), "\\s+")).as("words")
      )
      .groupBy($"words").count()
      .orderBy($"count".desc)
      .limit(10)


    result.printSchema()
    result.show()


    //Thread.sleep(100000)
    spark.stop()









  }

}
