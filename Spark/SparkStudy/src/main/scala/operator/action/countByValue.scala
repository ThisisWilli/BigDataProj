package operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.action
 * \* author: Willi Wei
 * \* date: 2019-09-24 10:16:10
 * \* description: 
 * \*/
object countByValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("countByKey")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val rdd1 = context.parallelize(List[(String, Int)](
      ("a", 100),
      ("a", 200),
      ("b", 2),
      ("b", 200))
    )
    val tupleToLong: collection.Map[(String, Int), Long] = rdd1.countByValue()
    tupleToLong.foreach(println)
  }
}
