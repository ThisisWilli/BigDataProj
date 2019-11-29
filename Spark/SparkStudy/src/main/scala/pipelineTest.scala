import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: 
 * \* author: Willi Wei
 * \* date: 2019-11-26 11:03:24
 * \* description: 验证先是一条数据处理到黑
 * \*/
object pipelineTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("data/data.txt")
    val rdd2 = rdd1.map(x => {
      println("****map****" + x)
      x + "~"
    })
    val rdd3 = rdd2.filter(one => {
      println(s"======filter===== $one")
      true
    })
    rdd3.count()
  }
}
