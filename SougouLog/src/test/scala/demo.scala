import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer

import java.util
import scala.collection.JavaConverters.iterableAsScalaIterableConverter


object demo {
  def main(args: Array[String]): Unit = {
    val terms: util.List[Term] = HanLP.segment("你好世界")
    println(terms)
    import scala.collection.JavaConversions._
    println(terms.asScala.map(term => term.word.trim))

    val terms1: util.List[Term] = StandardTokenizer.segment("端午++放假++屈原")
    println(terms1.asScala.map(_.word.replaceAll("\\+","")))







  }

}
