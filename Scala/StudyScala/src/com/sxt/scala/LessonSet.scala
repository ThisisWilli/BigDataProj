package com.sxt.scala
import scala.collection.mutable.Set
object LessonSet {
//  //创建
//  val set1 = Set(1,2,3,4,4)
//  val set2 = Set(1,2,5)
//  //遍历
//  //注意：set会自动去重
//  set1.foreach { print}
//  println()
//  for(s <- set1){
//    print(s)
//  }
//  println()
//  println("*******")
//  /**
//   * 方法举例
//   */
//
//  //交集
//  val set3 = set1.intersect(set2)
//  set3.foreach{print}
//  println()
//  val set4 = set1.&(set2)
//  set4.foreach{print}
//  println()
//  println("*******")
//  //差集
//  set1.diff(set2).foreach { println }
//  set1.&~(set2).foreach { println }
//  //子集
//  set1.subsetOf(set2)
//
//  //最大值
//  println(set1.max)
//  //最小值
//  println(set1.min)
//  println("****")
//
//  //转成数组，list
//  set1.toArray.foreach{println}
//  println("****")
//  set1.toList.foreach{println}
//
//  //mkString
//  println(set1.mkString)
//  println(set1.mkString("\t"))
  /**
   * 可变长Set
   * @param args
   */
  val set = Set[Int](1, 2, 3, 4, 5)
  set.add(100)
  set.+=(200)
  set.+=(1, 210, 300)
  def main(args: Array[String]): Unit = {

  }
}
