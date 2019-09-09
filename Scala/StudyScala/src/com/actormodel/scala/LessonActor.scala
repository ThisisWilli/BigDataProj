package com.actormodel.scala

import scala.actors.Actor

class myActor extends Actor{
  def act(): Unit ={
    while (true){
      receive{
        case x:String => println("get String = " + x)
        case x:Int => println("get Int")
        case _ => println("get default")
      }
    }
  }
}
object LessonActor {
  def main(args: Array[String]): Unit = {
    val actor = new myActor()
    actor.start()
    actor ! "I love scala"
  }
}
