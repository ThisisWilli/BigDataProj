package com.sxt.scala

object LessonMatch {
  def main(args: Array[String]): Unit = {
    val tuple = Tuple6(1,2,3f,4,"abc",55d)
    val tupleIterator = tuple.productIterator
    while(tupleIterator.hasNext){
      matchTest(tupleIterator.next())
    }

  }
  /**
   * 注意点：
   * 1.模式匹配不仅可以匹配值，还可以匹配类型
   * 2.模式匹配中，如果匹配到对应的类型或值，就不再继续往下匹配
   * 3.模式匹配中，都匹配不上时，会匹配到 case _ ，相当于default
   */
  def matchTest(x:Any) ={
    x match {
      case x:Int=> println("type is Int")
      case 1 => println("result is 1")
      case 2 => println("result is 2")
      case 3=> println("result is 3")
      case 4 => println("result is 4")
      case x:String => println("type is String")
      //      case x :Double => println("type is Double")
      case _ => println("no match")
    }
  }
}
