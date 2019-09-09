package com.sxt.scala

object LessonObject {
//  def fun (a: Int , b: Int ) : Unit = {
//    println(a+b)
//  }
//  fun(1,1)
//
//  def fun1 (a : Int , b : Int)= a+b
//  println(fun1(1,2))
//
//  def main(args: Array[String]): Unit = {
//
//  }
  def fun2(num:Int):Int={
  if (num == 1)
    return num
  else
    return num * fun2(num - 1)
}
  println(fun2(5))

  def main(args: Array[String]): Unit = {

  }
}
