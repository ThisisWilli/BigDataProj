package com.sxt.scala

object LessonImplicitTrans {
  def student(age:Int)(implicit name:String, i : Int){
    println(s"student : $name, age = $age, score = $i")
  }

  def Teacher(implicit name:String): Unit ={
    println(s"teacher name is = $name")
  }
//  def sayName(implicit name:String): Unit ={
//    println(name + " is a student")
//  }
  def main(args: Array[String]): Unit = {
    implicit val name:String = "zhangsan"
    implicit val sr = 100
    student(18)
    Teacher
  }
}
