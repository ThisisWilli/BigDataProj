package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package:
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 10:53:50
 * \* Description: flatmap算子，将原先一个RDD中的多个集合的元素合并成一个RDD中的一个集合，就是将原来的数组或者容器组合拆散
 * \*/
object flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("flatMap")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val lines = context.textFile("data/data.txt")
    // 如果这样被设置分隔符，那么则会将所有单词分割成一个个字符再输出，就是将一行元组全部分割
    println("如果不将元素分割直接输出：")
    lines.flatMap(one=>{
      one + "#"
    }).foreach(println)
    println()
    println("将元素分割之后再进行输出：")
    lines.flatMap(one=>{
      // 相当于传进去的函数
      one.split(" ")
    }).foreach(println)
    println("将元素直接输出")
    lines.foreach(println)
    println()
  }
}
