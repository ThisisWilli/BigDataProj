package com.sxt.scala

trait Read {
  val readType = "Read"
  val gender = "m"
  def read(name:String): Unit ={
    println(name + " is reading")
  }
}

trait Listen {
  val listenType = "Listen"
  val gender = "m"
  def listen(name:String): Unit ={
    println(name + "is listening")
  }
}

class Human() extends Read with Listen{
  override val gender = "f"
}

object test {
  //override val gender = " f"

  def main(args: Array[String]): Unit = {
    val person = new Human()
    person.read("zhangsan")
    person.listen("lisi")
    println(person.listenType)
    println(person.readType)
    println(person.gender)

  }
}
