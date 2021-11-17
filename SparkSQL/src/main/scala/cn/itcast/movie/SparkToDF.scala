package cn.itcast.movie

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//case class toDFRDD(id :Int,name:String,sex:String)

object SparkToDF {
  def main(args: Array[String]): Unit = {

    val sparktodf: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import sparktodf.implicits._


    val rdd: RDD[(Int, String, String)] = sparktodf.sparkContext.parallelize(
      List((1001, "zhangsan", "male"), (1003, "lisi", "male"), (1003, "xiaohong", "female"))
    )

    val dataframe: DataFrame = rdd.toDF("id", "name", "sex")
    dataframe.printSchema()
    dataframe.show()

    println("======================================")

    val seq: Seq[(Int, String, String)] = Seq(
      (1001, "zhangsan", "male"), (1003, "lisi", "male"), (1003, "xiaohong", "female")
    )

    val df: DataFrame = seq.toDF("id", "name", "sex")
    df.printSchema()
    df.show()

    
    
    
    
  }

}
