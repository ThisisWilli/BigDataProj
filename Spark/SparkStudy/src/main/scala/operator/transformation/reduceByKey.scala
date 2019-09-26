package operator.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-26 14:18:43
 * \* description: 将相同的key值得数据按照相应得逻辑进行处理，比如说将两个值合并成一个值
 * \*/
object reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("reduceByKey")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val rdd: RDD[(String, Int)] = sc.parallelize(List[(String, Int)](
      ("zhangsan", 100),
      ("zhangsan", 200),
      ("lisi", 250),
      ("lisi", 900)
    ))
    val pairWords: RDD[(String, Int)] = rdd.map(one => {
      (one._1, one._2)
    })
    pairWords.reduceByKey((v1:Int, v2:Int)=>(v1 + v2)).foreach(println)
  }
}
