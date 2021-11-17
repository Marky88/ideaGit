package app.mock

import java.util.{Properties, UUID}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
 * 模拟产生用户使用百度搜索引擎时，搜索查询日志数据，包含字段为：
 *      uid, ip, search_datetime, search_keyword
 */
object MockSearchLogs {
    
    def main(args: Array[String]): Unit = {
    
        // 搜索关键词，直接到百度热搜榜获取即可
        val keywords: Array[String] = Array(
            "广东新增1例东莞报告本土确诊", "全国累计接种新冠疫苗数破10亿", "女排收官战3比0完胜美国队",
            "外籍女子拒戴口罩冲乘客竖中指", "新华学院东莞校区2万师生不得离校", "外交部提醒中国公民尽早离开阿富汗",
            "3天前还参会的副市长主动投案", "朱婷被打到眼睛", "疑似被拐小女孩其实是小狗",
            "首都机场将采取更严格安检措施", "台湾新增107例本土病例", "央视曝光降糖神药为固体饮料",
            "离群独象水池边又秀球技又跳舞", "台湾62人接种疫苗后猝死", "洪水淹没养殖场 300头猪水中奔跑"
        )
        
        // 发送Kafka Topic
        val props = new Properties()
        props.put("bootstrap.servers", "node1.itcast.cn:9092")
        props.put("acks", "1")
        props.put("retries", "3")
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](props)
        
        val random: Random = new Random()
        while (true){
            // 随机产生一条搜索查询日志
            val searchLog: SearchLog = SearchLog(
                getUserId(), //
                getRandomIp(), //
                getCurrentDateTime(), //
                keywords(random.nextInt(keywords.length)) //
            )
            println(searchLog.toString)
            Thread.sleep(200 /*+ random.nextInt(100)*/)
            
           val record = new ProducerRecord[String, String]("search-log-topic", searchLog.toString)
           producer.send(record)
        }
        // 关闭连接
        producer.close()
    }
    
    /**
     * 随机生成用户SessionId
     */
    def getUserId(): String = {
        val uuid: String = UUID.randomUUID().toString
        uuid.replaceAll("-", "").substring(16)
    }
    
    /**
     * 获取当前日期时间，格式为yyyyMMddHHmmssSSS
     */
    def getCurrentDateTime(): String = {
        val format =  FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
        val nowDateTime: Long = System.currentTimeMillis()
        format.format(nowDateTime)
    }
    
    /**
     * 获取随机IP地址
     */
    def getRandomIp(): String = {
        // ip范围
        val range: Array[(Int, Int)] = Array(
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
        // 随机数：IP地址范围下标
        val random = new Random()
        val index = random.nextInt(10)
        val ipNumber: Int = range(index)._1 + random.nextInt(range(index)._2 - range(index)._1)
        //println(s"ipNumber = ${ipNumber}")
        
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
