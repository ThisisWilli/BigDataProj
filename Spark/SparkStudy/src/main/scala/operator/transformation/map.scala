package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 10:08:47
 * \* Description: map算子，将RDD中的每个数据项，通过map的函数映射成一个新的元素，
 *                 特点：输入一条数据，输出一条数据
 * \*/
object map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("map")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    // 读取数据
    val lines = sc.textFile("data/data.txt")
    lines.map(one=>{
      one + "#"
    }).foreach(println)
  }
}
