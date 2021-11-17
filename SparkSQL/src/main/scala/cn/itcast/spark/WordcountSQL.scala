package cn.itcast.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordcountSQL {

  def main(args: Array[String]): Unit = {

    val sparkwd: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import sparkwd.implicits._

    val inputwd: Dataset[String] = sparkwd.read.textFile("datas/input/wordcount.txt")

    inputwd.createTempView("tempwordcount")

    val resultDF: DataFrame = sparkwd.sql(
      """
        |select
        |  a.word, count(*) as wdcount
        |from
        |  (select explode(split(trim(value),"\\s+")) as word from tempwordcount) as a
        |group by a.word
        |""".stripMargin
    )

    resultDF.printSchema()
    resultDF.show()


    sparkwd.stop()
  }

}
