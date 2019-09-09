package com.sxt.scala

object LessonTrait2 {
  def main(args: Array[String]): Unit = {
    val p1 = new Point(1,2)
    val p2 = new Point(1,3)
    println(p1.isEqule(p2))
    println(p1.isNotEqule(p2))
  }
}

trait Equle{
  // 不知道会传什么类型所以传Any，isEqule没有实现，isNotEqule实现了，但是如果一个类继承了，那么就要实现这些方法
  def isEqule(x:Any) :Boolean
  def isNotEqule(x : Any)  = {
    !isEqule(x)
  }
}

class Point(x:Int, y:Int) extends Equle {
  val xx = x
  val yy = y

  def isEqule(p:Any) = {
    // 前面判断其是不是一个Point实例，如果时true，往后走
    p.isInstanceOf[Point] && p.asInstanceOf[Point].xx==xx
  }
}
