package com.sxt.scala

class Animal(name:String){
  def canFly(): Unit ={
    println(s"$name can fly....")
  }
}
class Rabbit1(xname : String){
  val name = xname
}

object LessonImplicitFunction {
  implicit def rabbitToAnimal(rabbit: Rabbit1):Animal = {
    new Animal(rabbit.name)
  }

  def main(args: Array[String]): Unit = {
    val rabbit = new Rabbit1("RABBIT")
    // 在作用域中寻找有没有隐式转换函数将rabbit转换成animal，如果有，就调用该函数
    rabbit.canFly()
  }
}
