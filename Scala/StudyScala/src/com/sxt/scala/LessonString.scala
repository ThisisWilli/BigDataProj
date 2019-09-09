package com.sxt.scala


/**
 * 1、String不可变，StringBuilder可变
 * 2、可变不可变只是建立在是否新建了一个变量
 */
object LessonString {
  var str = "abcd"
  var str1 = "ABCD"
  println(str.indexOf(97))
  println(str.indexOf("b"))

  def main(args: Array[String]): Unit = {

  }
}
