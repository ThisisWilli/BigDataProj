package operator.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-25 09:32:56
 * \* description: 
 * \*/
object combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("combineB")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val scoreList = Array(("ww1", 88), ("ww1", 95), ("ww2", 91), ("ww2", 93), ("ww2", 95), ("ww2", 98))

    //将2个学生成绩转为RDD，分2个partition存储
    val scoreRDD: RDD[(String, Int)] = sc.parallelize(scoreList, 2)
    println("[scoreRDD.partitions.size]：" + scoreRDD.partitions.size)
    //分区数，[scoreRDD.partitions.size]：2
    println("[scoreRDD.glom.collect]：" + scoreRDD.glom().collect().mkString(",")) //每个分区的内容

    //使用combineByKey，按每个学生累积分数和科目数量
    val rddCombineByKey: RDD[(String, (Int, Int))] = scoreRDD.combineByKey(v => (v, 1),
      (param1: (Int, Int), v) => (param1._1 + v, param1._2 + 1),
      (p1: (Int, Int), p2: (Int, Int)) => (p1._1 + p2._1, p1._2 + p2._2))
    println("[combineByKey]：" + rddCombineByKey.collect().mkString(","))
    //【combineByKey】：(ww2,(377,4)),(ww1,(183,2))

    //在map中使用case是scala的用法，按每个学生总成绩/科目数量，得到平均分
    val avgScore = rddCombineByKey.map { case (key, value) => (key, value._1 / value._2.toDouble) }
    println("[avgScore]：" + avgScore.collect().mkString(","))

  }
}
