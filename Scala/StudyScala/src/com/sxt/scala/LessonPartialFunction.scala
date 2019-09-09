package com.sxt.scala

object LessonPartialFunction {
  def MyTest:PartialFunction[String, String]={
      case "scala" => {"scala"}
      case "hello" => {"hello"}
      case _=>{"no match..."}
  }

  def main(args: Array[String]): Unit = {
    println(MyTest("scala"))
  }
}
