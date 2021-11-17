package cn.itcast.data

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JsonRead {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val inputDS: Dataset[String] = spark.read.textFile("datas/input/2015-03-01-11.json")
    inputDS.printSchema()
    inputDS.show(1,truncate = false)

    import org.apache.spark.sql.functions._
    val jsonDF: DataFrame = inputDS.select(
      get_json_object($"value", "$.id").as("id"),
      get_json_object($"value","$.type").as("type"),
      get_json_object($"value", "$.actor.url").as("url")
    )

    jsonDF.printSchema()
    jsonDF.show(10,truncate = false)

    




  }

}
