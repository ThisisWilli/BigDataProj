package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 09:30:15
 * \* Description:cartesian算子，对两个RDD内的所有元素进行笛卡尔积操作，操作后，内部实现返回cartesianRDD
 * \*/
object cartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("cartesian")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data1 = context.textFile("data/num1")
    val data2 = context.textFile("data/num2")
    data1.cartesian(data2).foreach(println)
  }
}
