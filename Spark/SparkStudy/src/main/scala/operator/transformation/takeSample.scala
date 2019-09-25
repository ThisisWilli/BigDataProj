package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-25 09:10:14
 * \* description:按照采样个数进行采样，返回结果不再是RDD，而是相当于对采样过后的数据进行collect()，返回结果的集合为单机的数组
 * \*/
object takeSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("takeSample")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val data = context.makeRDD(1 to 100, 3)
    val result: Array[Int] = data.takeSample(false, 2, 9L)
    for (i <- 0 until result.length){
      print(result(i))
      print(" ")
    }
  }
}
