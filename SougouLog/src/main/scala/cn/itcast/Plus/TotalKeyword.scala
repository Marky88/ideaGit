package cn.itcast.Plus

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.util


object TotalKeyword {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = getSparkSession(this.getClass)
    import spark.implicits._
    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/input/SogouQ.sample", minPartitions = 2)
    //RDD转换成DataFrame
    val dataframe: DataFrame = transDF(inputRDD, spark)

    //val spark: SparkSession = inputRDD.toDF().sparkSession  //反推出了spark

    //将下面3个业务放在一起进行了处理,并且开启了缓存
    threeReport(dataframe)

    //1.获取用户【查询词】，使用HanLP进行分词，按照单词分组聚合统计出现次数
  // keyWordCount(dataframe)

    //2.统计出每个用户每个搜索词点击网页的次数，可以作为搜索引擎搜索效果评价指标。
    // 先按照用户ID分组，再按照【查询词】分组，最后统计次数，求取最大次数、最小次数及平均次数。
   // uerKeyWordCount(dataframe)

    //按照【访问时间】字段获取【小时】，分组统计各个小时段用户查询搜索的数量，进一步观察用户喜欢在哪些时间段上网，使用搜狗引擎搜索
  //  hourUserCount(dataframe)

  }

    //创建SparkSession
  def  getSparkSession(clazz:Class[_]):SparkSession={
      SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master("local[2]")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }


 //将RDD转换成DataFrame
  def transDF(input:RDD[String],spark:SparkSession): DataFrame ={
    import spark.implicits._
    val transdf: DataFrame = input
      .filter(lines => null != lines && lines.trim.split("\\s+").length == 6)
      .map(line => {
        val arr: Array[String] = line.trim.split("\\s+")
        emp(arr(0), arr(1),
          arr(2).replaceAll("\\[", "").replaceAll("\\]", ""),
          arr(3).toInt, arr(4).toInt, arr(5))
      }
      ).toDF
    transdf
  }


  def threeReport(dateframe:DataFrame)={

    //多个业务,开启缓存数据
    dateframe.persist(StorageLevel.MEMORY_AND_DISK)

    keyWordCount(dateframe)

    uerKeyWordCount(dateframe)

    hourUserCount(dateframe)

    //数据不在使用释放资源
    dateframe.unpersist()

  }


  // 1.获取用户【查询词】，使用HanLP进行分词，按照单词分组聚合统计出现次数
  def keyWordCount(df: DataFrame):Unit={
      val spark: SparkSession = df.sparkSession
      import spark.implicits._

      val to_hanlp: UserDefinedFunction = udf(
        (queryWords: String) => {
          val terms: util.List[Term] = HanLP.segment(queryWords.trim)
          // 导入隐式转，将Java中列表转换为Scala中列表
          import scala.collection.JavaConversions._
          terms.map(line => line.word).toArray
        }
      )

      //使用DSL编程 从这里开始
      val dslDF: DataFrame = df.select(
        explode(to_hanlp($"queryWords")).as("words")
      )
        .groupBy($"words")
        .count()
        .orderBy($"count".desc)
        .limit(10)

     // dslDF.printSchema()
     // dslDF.show(10,truncate = false)
    }

  //2.统计出每个用户每个搜索词点击网页的次数，可以作为搜索引擎搜索效果评价指标
  //最后统计次数，求取最大次数、最小次数及平均次数。
 def  uerKeyWordCount(df: DataFrame)= {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val pageCount: DataFrame = df
      .select($"userId", $"queryWords")
      .groupBy($"userId", $"queryWords")
      .agg(count($"queryWords").as("total")) //直接使用count不能重新命名,默认为count 修改需要添加agg()
      .orderBy($"total".desc)

    val aggDF: DataFrame = pageCount.agg(
      max($"total").as("maxCount"),
      min($"total").as("minCount"),
      round(avg($"total"), 2).as("avgCount")
    )

    pageCount.schema
    pageCount.show(10, truncate = false)

    aggDF.printSchema()
    aggDF.show()

  }
    //按照【访问时间】字段获取【小时】，分组统计各个小时段用户查询搜索的数量，
    // 进一步观察用户喜欢在哪些时间段上网，使用搜狗引擎搜索
    //00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/
    def  hourUserCount(df:DataFrame) ={

      val spark: SparkSession = df.sparkSession
      import spark.implicits._

      val merHour: UserDefinedFunction = udf(
        (queryTime: String) => {
          val visitHour: String = queryTime.substring(3, 5)
          visitHour
        }
      )

      val resultDF: DataFrame = df
        .select(
          merHour($"queryTime").as("perHour"),
          $"userId"
        )
        .groupBy($"perHour")
        .agg(count($"userId").as("userCount"))
        .orderBy($"userCount".desc)

      resultDF.printSchema()
      resultDF.show(10,truncate = false)

     }

}



case class emp(
                queryTime:String,
                userId:String,
                queryWords:String,
                resultRank:Int,
                clickRank:Int,
                clickUrl:String
              )