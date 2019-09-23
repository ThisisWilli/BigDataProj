package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 09:58:02
 * \* Description: 
 * \*/
object filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("filter")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data = context.textFile("data/data2.txt")
//    val rdd = data.flatMap(one=>{
//      one.split(" ")
//    }).filter(one=>{
//      "hello".equals(one)
//    }).foreach(println)
    val rdd = data.map(one=>one).filter(one=>one.contains("spark")).foreach(println)
  }
}
