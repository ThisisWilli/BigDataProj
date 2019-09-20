package com.study.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 在一个边长为2的正方形内画个圆，正方形的面积 S1=4，圆的半径 r=1，面积 S2=πr^2=π现在只需要计算出S2就可以知道π，<br>
 * 这里取圆心为坐标轴原点，在正方向中不断的随机选点，总共选n个点，
 * <br>计算在圆内的点的数目为count，则 S2=S1*count/n，然后就出来了<br>
 * -- 蒙特-卡罗方法
 */
object SparkPi {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    // val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf);
    spark.setLogLevel("Error")
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = spark.parallelize(1 to n,slices).map({ i =>
      /** Returns a `double` value with a positive sign, greater than or equal
       *  to `0.0` and less than `1.0`.
       */
      def random: Double = java.lang.Math.random()
      //这里取圆心为坐标轴原点，在正方向中不断的随机选点
      val x = random * 2 - 1
      val y = random * 2 - 1
      println(x+"--"+y)
      //通过在圆内的点
      if (x*x + y*y < 1) 1 else 0

    }).reduce(_ + _)

    //pi=S2=S1*count/n
    println("Pi is roughly " + 4.0 * count / n)


    spark.stop()
  }
}
