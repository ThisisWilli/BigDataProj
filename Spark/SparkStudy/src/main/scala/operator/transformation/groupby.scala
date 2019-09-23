package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 09:36:30
 * \* Description: groupby算子，将元素通过函数生成相应的key，数据转化成key-value格式，之后将key相同的元素分为一组
 * \*/
object groupby {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("groupby")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data = context.textFile("data/key_value")
    data.map(one=>{
      one.split(" ")
    }).map(f=>(f(0), f(1))).groupBy(f=>f._1).foreach(println)
  }
}
