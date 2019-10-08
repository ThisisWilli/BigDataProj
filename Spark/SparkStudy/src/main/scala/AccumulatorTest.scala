import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * \* project: SparkStudy
 * \* package:
 * \* author: Willi Wei
 * \* date: 2019-10-08 10:48:15
 * \* description:
 * \*/
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val accumulator = sc.longAccumulator
    var i = 0
    val rdd1: RDD[String] = sc.textFile("data/data2.txt")
    val rdd2: RDD[String] = rdd1.map(one => {
//      i += 1
//      println(s"Executor i = $i")
      accumulator.add(1)
      // 两种方法打印都可以
      // println(s"Executor accumulator = ${accumulator}")
      println(s"Executor accumulator = ${accumulator.value}")
      one
    })
    rdd2.collect()
    // println(s"i = $i")
    println(s"accumulator = ${accumulator}")
  }
}
