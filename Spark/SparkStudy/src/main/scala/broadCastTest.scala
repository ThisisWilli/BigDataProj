import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: 
 * \* author: Willi Wei
 * \* date: 2019-10-08 10:17:43
 * \* description: 
 * \*/
object broadCastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val list = List[String]("zhangsan", "lisi")
    val bcList: Broadcast[List[String]] = sc.broadcast(list)
    val nameRDD = sc.parallelize(List[String]("zhangsan", "lisi", "wangwu"))
    val result = nameRDD.filter(name => {
      // 拿回来之后的广播变量，还原了List
      val innerList: List[String] = bcList.value
      // !list.contains(name)
      // 这样使用的是广播变量，内存也能减少很多
      !innerList.contains(name)
    })
    result.foreach(println)
  }
}
