package operator.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 09:53:54
 * \* description:将两个RDD按照kv格式的数据压缩到一起，但是两个RDD数据的个数应该相同， 一一对应
 * \*/
object zip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("zip")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val rdd1 = context.parallelize(List[String]("zhangsan", "lisi", "wangwu", "maliu"))
    val rdd2 = context.parallelize(List[String]("100", "200", "300", "400"))
    val result: RDD[(String, String)] = rdd1.zip(rdd2)
    result.foreach(println)
    val result2: RDD[(String, Long)] = rdd1.zipWithIndex()
    result2.foreach(println)
  }
}
