package cn.itcast.log.search

case class SougouRecord(
                       queryTime:String,
                       userId:String,
                       queryWords:String,
                       resultRank:Int,
                       clickRank:Int,
                       clickUrl:String
                       )
