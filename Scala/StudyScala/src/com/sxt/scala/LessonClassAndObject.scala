package com.sxt.scala

/**
 * Scala                                                                          l
 * 1.Scala Object 相当于java中的单例，Object中定义的全是静态的,相当于java的工具类，Object不可传参,对象要传参，使用apply方法
 * 2.Scala中定义变量使用var，定义常量使用val，变量可变，常量不可变，变量和常量类型可以省略不写，会自动推断
 * 3.Scala中每行后面都有分号自动推断机制，不用显式写出";"
 * 4.Scala中命名建议驼峰
 * 5.Scala类中可以传参，传参一定要指定类型，有了参数就默认有了构造，类中的属性默认有getter和setter方法
 * 6.类中重写构造时，构造中第一行必须先调用默认的构造   def this(){....}
 * 7.Scala中当new class时，类中除了方法不执行【除了构造方法】其他都执行
 * 8.在同一Scala文件中，class名称和Object名称一样是，这个类叫做对象的半生类，这个对象叫做这个类的半生对象，他们之间可以互相访问私有变量
 */
class Person(xname:String, xage:Int){
  val name = xname;
  val age = xage
  var gender = 'F'

//  println("*********Person Class************")
  def this(yname:String, yage:Int, ygender:Char){
    this(yname, yage)
    this.gender = ygender
  }

  def sayName()={
    println("hello world...." + LessonClassAndObject.name)
  }
//  println("==========Person Class============")
}

object LessonClassAndObject {

  def apply(i:Int) = {
    println("Score is " + i)
  }

  val name = "wangwu"
  def main(args: Array[String]): Unit = {
    LessonClassAndObject(1000)
//    val a = 100;
//    val b = 200;
//    println(b)
//    val p = new Person("zhangsan", 20)
//    println(p.gender)
//    val p1 = new Person("diaochan", 18, 'F')
//    println(p1.gender)
//    println(p.name)
//    println(p.age)
//    p.sayName()
  }
}
