package com.sxt.scala

class Rabbit(s:String){val name = s}
object LessonImplicitClass {
  implicit class Animal(rabbit: Rabbit){
    val tp = "Animal"
    def canFly() ={
      println(rabbit.name + " can fly...")
    }
  }

  def main(args: Array[String]): Unit = {
    val rabbit = new Rabbit("rabbit")
    rabbit.canFly()
    println(rabbit.tp)
  }
}

