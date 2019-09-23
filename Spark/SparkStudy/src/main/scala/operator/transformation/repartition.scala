package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 10:59:53
 * \* Description: repartition可以增多分区，也可以减少分区,注意数据流向，可以增多分区，是一个宽依赖
 * \*/
object repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("mapPartitionWithIndex")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val rdd1 = context.parallelize(List[String](
      "love1", "love2", "love3", "love4",
      "love5", "love6", "love7", "love8",
      "love9", "love10", "love11", "love12"
    ), 3)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, iter)=>{
      val list = new ListBuffer[String]()
      while (iter.hasNext){
        val one = iter.next()
        list.+=(s"rdd2 partition = [$index], value = [$one]")
      }
      list.iterator
    })
    val rdd3 = rdd2.repartition(4)
    rdd3.mapPartitionsWithIndex((index, iter)=>{
      val list = new ListBuffer[String]()
      while (iter.hasNext){
        val one = iter.next()
        list.+=(s"rdd3 partition = [$index], value = [$one]")
      }
      list.iterator
    }).foreach(println)
  }
}
