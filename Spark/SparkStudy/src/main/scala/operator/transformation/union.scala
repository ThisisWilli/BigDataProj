package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 20:03:38
 * \* Description: union算子，保证两个RDD数据类型相同，将两个RDD中的数值进行笛卡尔积运算，将结果放到一个新的RDD中去，其实就是将两个RDD
 *                  合在一起并且不去重
 * \*/
object union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("union")
    val context = new SparkContext(conf)
    context.setLogLevel("Error")
    val sc1 = context.textFile("data/num1")
    val sc2 = context.textFile("data/num2")
    val union = sc1.union(sc2)
    union.map(one=>{
      one + "#"
    }).foreach(println)
  }
}
