package com.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Operate {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val lines = sc.textFile("D:\\BigDataProj\\Spark\\SparkWC\\data.txt")

//    val result : Array[String] = lines.collect()
//    result.foreach(println)
//    val l : Long = lines.count()
//    println(l)
    val words = lines.flatMap(one=>{one.split(" ")})

    val pairWords = words.map(one=>{(one, 1)})
//    //val reduceResult =  pairWords.reduceByKey((v1:Int, v2:Int)=>{v1 + v2})
    val reduceResult :RDD[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int) => (v1 + v2))
//    val transRDD : RDD[(Int,String)] = reduceResult.map(tp=>{tp.swap})
//    val result : RDD[(Int, String)] = transRDD.sortByKey(false)
//    result.map(_.swap).foreach(println)
//    val result : RDD[String] = lines.sample(true, 0.1, 28L) // 进行抽样
//    result.foreach(println)
//    reduceResult.sortBy(tp=>{tp._2}).foreach(println)
    reduceResult.foreach(println)
//    reduceResult.sortByKey()
    // map 一对一
//    lines.map(one=>{
//      one + "#"
//    }).foreach(println)
    // flatmap 一对多
//    lines.flatMap(one=>{one.split(" ")}).foreach(println)
    // filter
//    val rdd1 = lines.flatMap(one=>{one.split(" ")})
//    rdd1.filter(one=>{
//      "hello".equals(one)
//    }).foreach(println)
  }
}
