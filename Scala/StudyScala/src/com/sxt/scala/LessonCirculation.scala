package com.sxt.scala

object LessonCirculation {
  def main(args: Array[String]): Unit = {
    /**
     * if else
     */
//    val age = 20;
//    if (age <=20){
//      println("age < 20")
//    }else if (age > 20 && age <= 30){
//      println("20")
//    }
    /**
     * for
     */
//    var r = 1 to 10 //包含值
//    var u = 1 until 10 //不包含值
//    println(r)
//    println(u)
//      println(1.to(10, 2))
//      println(1.until(10, 1))
//    var i = 0;
//    // 单层循环，可以将其看成一个赋值符号
//    for (i <- 1 until 10){
//     println(i)
//    }
//    // 双层循环
//    for (i <- 0 to 10; j <- 0 to 10 ){
//      println(i, j)
//    }

    //例子： 打印小九九
//    for(i <- 1 until 10 ;j <- 1 until 10){
//      if(i>=j){
//        print(i +" * " + j + " = "+ i*j+"	")
//
//      }
//      if(i==j ){
//        println()
//      }
//    }
    //可以在for循环中加入条件判断
//    for(i<- 1 to 10 ;if (i%2) == 0 ;if (i % 4 == 0) ){
//      println(i)
//    }
    val list = for (i <- 1 to 10; if (i > 5)) yield i
    println(list)
  }
}
