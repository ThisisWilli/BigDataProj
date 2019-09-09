package com.sxt.scala
import scala.collection.mutable.Map
object LessonMap {
//  val map = Map(
//    "1" -> "bjsxt",
//      2 -> "shsxt",
//      (3,"xasxt")
//  )
//  //获取值
//  println(map.get("1").get)
//  val result = map.get(8).getOrElse("no value")
//  println(result)
  //map遍历
//  for(x <- map){
//    println("====key:"+x._1+",value:"+x._2)
//  }
//  map.foreach(f => {
//    println("key:"+ f._1+" ,value:"+f._2)
//  })
//遍历key
//  val keyIterable = map.keys
//  keyIterable.foreach { key => {
//    println("key:"+key+", value:"+map.get(key).get)
//  } }
//  println("---------")
//遍历value
//  val valueIterable = map.values
//  valueIterable.foreach { value => {
//    println("value: "+ value)
//  } }
//合并map
//  val map1 = Map(
//    (1,"a"),
//    (2,"b"),
//    (3,"c")
//  )
//  val map2 = Map(
//    (1,"aa"),
//    (2,"bb"),
//    (2,90),
//    (4,22),
//    (4,"dd")
//  )
//  map1.++:(map2).foreach(println)
  /**
   * map方法
   */
  //count
//  val countResult  = map.count(p => {
//    p._2.equals("shsxt")
//  })
//  println(countResult)
//
//  //filter
//  map.filter(_._2.equals("shsxt")).foreach(println)
//
//  //contains
//  println(map.contains(2))
//
//  //exist
//  println(map.exists(f =>{
//    f._2.equals("xasxt")
//  }))
  /**
   * 可变长map
   * @param args
   */

  val map = Map[String, Int]()
  map.put("hello", 100)
  map.put("world", 200)
  //map遍历
  for(x <- map){
      println("====key:"+x._1+",value:"+x._2)
  }


  def main(args: Array[String]): Unit = {

  }
}
