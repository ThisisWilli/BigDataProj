package operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.action
 * \* author: Willi Wei
 * \* date: 2019-09-24 10:07:04
 * \* description: reduce算子
 * \*/
object reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("reduce")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val rdd1 = context.parallelize(List[Int](1, 2, 3, 4, 5))
    val result: Int = rdd1.reduce((v1, v2)=>{v1 + v2})
    println(result)
  }
}
