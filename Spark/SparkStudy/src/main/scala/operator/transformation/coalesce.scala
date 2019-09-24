package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 09:40:04
 * \* description: coalesce算子，可以增多分区，也可以减少分区，常用于减少分区
 *      coalesce由少的分区分到多的分区时，不让产生shuffle，不起作用
 * \*/
object coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("coalesce")
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
    // 如果让shuffle执行，则会产生宽依赖，如果不执行，如果不执行，可能会产生空分区
    val rdd3 = rdd2.coalesce(4)
    println(s"rdd3 partition number:${rdd3.getNumPartitions}")
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
