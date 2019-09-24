package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 10:03:04
 * \* description: 
 * \*/
object groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("groupbykey")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val rdd1 = context.parallelize(List[(String, Int)](("a", 100), ("a", 200), ("b", 2), ("b", 200)))
    rdd1.groupByKey().foreach(println)
  }
}
