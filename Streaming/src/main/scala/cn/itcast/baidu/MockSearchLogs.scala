package cn.itcast.baidu

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.{Properties, UUID}
import scala.util.Random


/**
 * 模拟产生用户使用百度搜索引擎时，搜索查询日志数据，包含字段为：
 *      uid, ip, search_datetime, search_keyword
 */
object MockSearchLogs {

  def main(args: Array[String]): Unit = {
    val keywords :Array[String] =Array(
        "吴尊友提醒五一不参加大型聚集聚会",
        "称孩子没死就得购物导游被处罚",
        "刷视频刷出的双胞胎姐妹系同卵双生",
        "云公民受审认罪 涉嫌受贿超4.6亿",
        "印度男子下跪求警察别拿走氧气瓶",
        "广电总局:支持查处阴阳合同等问题",
        "75位一线艺人注销200家关联公司",
        "空间站天和核心舱发射成功",
        "中国海军舰艇警告驱离美舰",
        "印度德里将狗用火葬场改为人用",
        "公安部派出工作组赴广西",
        "美一男子遭警察跪压5分钟死亡",
        "华尔街传奇基金经理跳楼身亡",
        "阿波罗11号宇航员柯林斯去世",
        "刘嘉玲向窦骁何超莲道歉"
    )

    // 发送Kafka Topic
    val props = new Properties()
    props.put("bootstrap.servers", "node1.itcast.cn:9092")
    props.put("ack","1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer",classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    val random = new Random()
    while(true){

      val searchLog: SearchLog = SearchLog(
        getUserId(), //
        getRandomIp(), //
        getCurrentDateTime(), //
        keywords(random.nextInt(keywords.length)) //
      )

      println(searchLog.toString)
      Thread.sleep(100 + random.nextInt(100))

      val record = new ProducerRecord[String, String]("search-log-topic", searchLog.toString)
      producer.send(record)
    }

  }

  def getUserId():String={
    val uuid: String = UUID.randomUUID().toString
    uuid.replaceAll("-","").substring(16)
  }

  def getCurrentDateTime():String={
    val dateformat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ssSSS")
    val nowDateTime: Long = System.currentTimeMillis()
    dateformat.format(nowDateTime)
  }


  def getRandomIp():String={
    val range :Array[(Int,Int)] = Array(
      (607649792,608174079), //36.56.0.0-36.63.255.255
      (1038614528,1039007743), //61.232.0.0-61.237.255.255
      (1783627776,1784676351), //106.80.0.0-106.95.255.255
      (2035023872,2035154943), //121.76.0.0-121.77.255.255
      (2078801920,2079064063), //123.232.0.0-123.235.255.255
      (-1950089216,-1948778497),//139.196.0.0-139.215.255.255
      (-1425539072,-1425014785),//171.8.0.0-171.15.255.255
      (-1236271104,-1235419137),//182.80.0.0-182.92.255.255
      (-770113536,-768606209),//210.25.0.0-210.47.255.255
      (-569376768,-564133889) //222.16.0.0-222.95.255.255
      )
    val random = new Random()
    val index: Int = random.nextInt(10)
    val ipNumber: Int = random.nextInt(range(index)._2 - range(index)._1)
    // 转换Int类型IP地址为IPv4格式
    number2IpString(ipNumber)
    }

  /**
   * 将Int类型IPv4地址转换为字符串类型
   */
  def number2IpString(ip: Int): String = {
    val buffer: Array[Int] = new Array[Int](4)
    buffer(0) = (ip >> 24) & 0xff
    buffer(1) = (ip >> 16) & 0xff
    buffer(2) = (ip >> 8) & 0xff
    buffer(3) = ip & 0xff
    // 返回IPv4地址
    buffer.mkString(".")
  }



}
