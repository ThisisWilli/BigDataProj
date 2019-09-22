package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 19:44:55
 * \* Description: glom算子，将每个分区形成一个数组，内部实现是返回的GlommedRDD
 * \*/
object glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("glom")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val lines = context.makeRDD("data/num2")
    val list = lines.glom().collect()
    //list.foreach(x=>print())
  }
}
