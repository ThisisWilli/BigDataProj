package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 10:50:39
 * \* Description: 
 * \*/
object mapPartitionWithIndex {
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
        list.+=(s"rdd1 partition = [$index], value = [$one]")
      }
      list.iterator
    })
    rdd2.foreach(println)
  }
}
