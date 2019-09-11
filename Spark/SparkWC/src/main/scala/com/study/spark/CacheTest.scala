package com.study.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object CacheTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cache")
    val sc = new SparkContext(conf)
    var rdd: RDD[String] = sc.textFile("D:\\persistData.txt")
    rdd = rdd.cache()
    var startTime1: Long = System.currentTimeMillis()
    // 由于cache是懒执行，所有第一次count时，是从磁盘中找数据
    val result1 = rdd.count()
    var endTime1: Long = System.currentTimeMillis()
    println(s"在磁盘中读=$result1,time=${endTime1 - startTime1}ms")

    var startTime2: Long = System.currentTimeMillis()
    // 第二次找数据时，数据已经通过cache()放入内存中，直接从内存中读取数据
    val result2 = rdd.count()
    var endTime2: Long = System.currentTimeMillis()
    println(s"在内存中读=$result2,time=${endTime2 - startTime2}ms")

    sc.stop()
  }
}
