package cn.itcast.baidu

case class SearchLog(
                    sessionId:String,
                    ip:String,
                    datetime:String,
                    keyword:String
                    ){
  override def toString: String = s"$sessionId,$ip,$datetime,$keyword"
}

