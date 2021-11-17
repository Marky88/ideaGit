package cn.itcast.spark

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author: Marky
 * @date: 2021/11/10 22:34
 * @description:
 */
object imei_month {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val inputDS: Dataset[String] = spark.read.textFile("datas/input/data/imei.txt")

    inputDS.show(10)


    val output: Dataset[String] = inputDS.map(line => line.trim)

    output.write.format("text")
      .save("datas/output/out_imei")


    spark.stop()


  }

}
