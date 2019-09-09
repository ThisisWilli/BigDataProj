package com.sxt.scala

import scala.collection.mutable.ListBuffer

object LessonList {
  //创建
//  val list = List(1,2,3,4,5)
//
//  //遍历
//  list.foreach { x => print(x)}
//  println()
//  //    list.foreach { println}
//  //filter , 滤除掉使函数返回false的元素
//  val list1  = list.filter { x => x>3 }
//  list1.foreach { print}
//  println()
//  //count
//  val value = list1.count { x => x>3 }
//  println(value)
//
//  //map
//  val nameList = List(
//    "hello bjsxt",
//    "hello xasxt",
//    "hello shsxt"
//  )
//  val mapResult:List[Array[String]] = nameList.map{ x => x.split(" ") }
//  mapResult.foreach{println}
//
//  //flatmap
//  val flatMapResult : List[String] = nameList.flatMap{ x => x.split(" ") }
//  flatMapResult.foreach { println }
//
  /**
   * 可变长list
   * @param args
   */

  val listBuffer: ListBuffer[Int] = ListBuffer[Int](1, 2, 3, 4, 5)
  listBuffer.append(6, 7, 8, 9) //追加元素
  listBuffer.+=(10) //在后面追加元素
  listBuffer.+=(100) //在开头追加元素
  listBuffer.foreach(print)
  def main(args: Array[String]): Unit = {

  }
}
