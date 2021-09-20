package func.agg

object ScalaAggTest {
	
	def main(args: Array[String]): Unit = {
		
		// 定义一个列表
		val numbers: List[Int] = (1 to 10).toList
		println(numbers)
		
		// reduce, TODO: 聚合函数，对多条数据聚合，需要聚合中间临时变量
		/*
			def reduce[A1 >: A](op: (A1, A1) => A1): A1
									中间临时变量
										集合中每个元素
			
			对列表List中数据求和，中间临时变量：tmp： Int
			
			TODO： reduce函数中，中间临时变量的值为列表中第一个元素的值
		 */
		val sumValue: Int = numbers.reduce((tmp, item) => {
			println(s"tmp = ${tmp}, item = ${item}, sum = ${tmp + item}")
			// 中间临时变量与集合中元素求和
			tmp + item
		})
		println(s"sumValue = ${sumValue}")
		
		// numbers.reduce(_ + _)
		
		// TODO: 发现fold函数，相比较reduce函数来说，多1个参数
		/*
			def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): A1
			
			zero：初始化，中间临时变量的值
				用于用户初始化变量的值
		 */
		val foldSum = numbers.fold(0)((tmp, item) => {
			println(s"tmp = ${tmp}, item = ${item}, sum = ${tmp + item}")
			// 中间临时变量与集合中元素求和
			tmp + item
		})
		println(s"foldSum = ${foldSum}")
	}
	
}
