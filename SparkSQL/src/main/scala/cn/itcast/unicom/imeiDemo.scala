package cn.itcast.unicom

import org.apache.spark.sql.{Dataset, SparkSession}

import java.io

/**
 * @author: Marky
 * @date: 2021/10/20 21:52
 * @description:
 */
object imeiDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("imeiDemo")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val inputDS: Dataset[String] = spark.read.textFile("datas/input/data/imei.log")
     inputDS.flatMap(line => {
      val arr: Array[String] = line.split("\\|")
      val imeiPre: String = arr(0)
      val imeiFront: String = arr(1)

      val arr1: Array[String] = imeiFront.split("#")
      val idType: String = arr1(0).substring(0, 1) // 1或者0
      val str1: String = arr1(0).substring(1) //051,010,...

/*        val tuples: Array[(String, String, Int, Int)] = arr1.slice(1, arr1.length).map(str => {
          val urlDetail = str.split(",")
          val urlId: String = urlDetail(0)
          val count3 = urlDetail(1).toInt
          val count4 = urlDetail(2).toInt
                val tuple: (String, String, Int, Int) = idType match {
                  case "1" => (idType, urlId, count3, count4)
                  case "0" => (idType, urlId, count4, count3)
                }
                tuple
        })
        tuples*/

      val str2: Array[String] = arr1(1).split(",")
      val url = str2(0)
      val count1 = str2(1).toInt
      val count2 = str2(2).toInt

       val tuple: (String, Int, Int) = idType match {
         case "1" => (idType, count1, count2)
         case "0" => (idType, count2, count1)
       }

       Array(tuple)

    })
     .toDF("idType", "url_id","count3").createOrReplaceTempView("tmpView")
 // value.show(19)
 //   value.toDF("idType", "url_id","count3").createOrReplaceTempView()

//    value.show()


    spark.sql(
      s"""
         |
         |select
         |*
         |from
         |tmpView
         |
         |""".stripMargin).show(10)


    spark.stop()

      val tuple4 = (1,3,5,"ew")
    print("tuple4: "+tuple4)

    val arr: Array[(Int, Int, Int, String)] = Array(tuple4)
    print("arr: "+arr)






  }

}
