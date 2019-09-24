package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 09:12:35
 * \* description: distinct算子，对RDD中的数据进行去重,但是数据出来是无序的
 * \*/
object distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("distinct")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data = context.parallelize(List[Int](1, 2, 2, 3, 4, 4, 5, 5, 6, 6, 7))
    data.distinct().foreach(println)
  }
}
