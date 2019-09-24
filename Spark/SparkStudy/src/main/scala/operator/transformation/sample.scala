package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 09:29:49
 * \* description: sample随机抽样算子，，根据传进去的小数按比例进行有放回或者无放回的抽样
 * \*/
object sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("sample")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data = context.textFile("data/data2.txt")
    data.sample(true, 0.1, 9).foreach(println)
  }
}
