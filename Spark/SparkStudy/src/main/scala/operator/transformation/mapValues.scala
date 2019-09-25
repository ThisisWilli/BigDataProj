package operator.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-25 09:23:45
 * \* description:mapValue算子，针对k-v型数据中的value进行map操作，而不对key进行处理，
 * \*/
object mapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("mapValues")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val result: RDD[(String, Int)] = context.parallelize(List[(String, Int)](
      ("zhangsan", 100),
      ("zhangsan", 300),
      ("lisi", 150),
      ("lisi", 250)
    ))
    result.mapValues(a=> a + 2).foreach(println)

  }
}
