package com.sxt.scala

import scala.collection.mutable.ArrayBuffer

object LessonShuZu {
//  //创建类型为int，长度为3的数组
//  var arr1 = new Array[Int](3)
//  //创建String类型的数组，直接赋值
//  val arr2 = Array[String]("s100", "s200", "s300")
//  //println(arr2.toString)
//  //赋值
//  arr1(0) = 100
//  arr1(1) = 200
//  arr1(2) = 300
//
//  /**
//   * 遍历的两种方式
//   * @param args
//   */
//    for (i <- arr1){
//      println(i)
//    }
//    arr1.foreach(i => {
//      println(i)
//    })
//    for (s <- arr2){
//      println(s)
//    }
//    arr2.foreach{
//      x => println(x)
//    }

//  /**
//   * 创建二维数组和遍历
//   * @param args
//   */
//  val arr3 = new Array[Array[String]](3)
//  arr3(0)=Array("1","2","3")
//  arr3(1)=Array("4","5","6")
//  arr3(2)=Array("7","8","9")
//  for (i <- 0 until arr3.length){
//    for (j <- 0 until arr3(i).length){
//      print(arr3(i)(j) + " ")
//    }
//    println()
//  }
//  var count = 0
//  for(arr <- arr3 ;i <- arr){
//    if(count%3 == 0){
//      println()
//    }
//    print(i+"	")
//    count +=1
//  }
//
//  println()
//  arr3.foreach { arr  => {
//    arr.foreach { println }
//  }}
//
//
//  val arr4 = Array[Array[Int]](Array(1,2,3),Array(4,5,6))
//  arr4.foreach { arr => {
//    arr.foreach(i => {
//      println(i)
//    })
//  }}
//  println("-------")
//  for(arr <- arr4;i <- arr){
//    println(i)
//  }
  val arr = ArrayBuffer[String]("a", "b", "c")
  arr.append("hello", "scala") //添加多个元素
  arr. +=("end") //在最后添加元素
  arr. +=:("start") //在开头添加元素
  arr.foreach(println)
  def main(args: Array[String]): Unit = {

  }
}
