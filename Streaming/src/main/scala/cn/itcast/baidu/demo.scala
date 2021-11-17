package cn.itcast.baidu

object demo {
  def main(args: Array[String]): Unit = {


     val a: (Int, Int) => Int  = (x:Int, y:Int)=> (x+y)

//     val a =(x:Int,y:Int)=> (x+y)


    val m= Map("name" ->"allen","age" ->"18")

    //根据key去map中取值： 有值  null
    val maybeString: Option[String] = m.get("name")

    println(maybeString)






  }

}
