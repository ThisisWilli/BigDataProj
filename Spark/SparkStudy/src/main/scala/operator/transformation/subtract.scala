package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 09:21:00
 * \* description: subtract算子，取出RDD1和RDD2交集中的所有元素，返回出现在data2中但不在data1中出现的元素
 * \*/
object subtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("subtract")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data1 = context.parallelize(List[Int](1, 2, 3, 4, 5))
    val data2 = context.parallelize(List[Int](1, 2, 3, 4, 5, 6, 7, 8))
    val data3 = data2.subtract(data1)
    data3.collect().foreach(println)
  }
}
