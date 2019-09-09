package com.sxt.scala

object LessonCaseLesson {
  case class Person1(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val p1 = new Person1("zhangsan",10)
    val p2 = Person1("lisi",20)
    val p3 = Person1("wangwu",30)

    val list = List(p1,p2,p3)
    list.foreach { x => {
      x match {
        case Person1("zhangsan",10) => println("zhangsan")
        case Person1("lisi",20) => println("lisi")
        case _ => println("no match")
      }
    }
    }
  }
}
