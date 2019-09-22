package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 11:15:45
 * \* Description: mapPartition获取到每个分区的迭代器，再函数中通过这个分区整体的迭代器对整个分区的元素进行操作
 * \*/
object mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("mapPartitions")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val num1 = context.textFile("data/num1")
    val num2 = context.textFile("data/num2")
    num1.mapPartitions(one=>{
      // one的类型是Iterator[String]先对其用filter，然后再将它转化为int，再去过滤
      one.filter(_.toInt >= 3)
    }).foreach(println)
    num2.mapPartitions(one=>{
      one.filter(_.toInt >= 3)
    }).foreach(println)
  }
}
