package com.study.pvuv

import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import java.io.File

//import scala.reflect.io.File
import scala.util.Random

/**
 * \* project: SparkWordCount
 * \* package: com.study.pvuv
 * \* author: Willi Wei
 * \* date: 2019-09-25 10:07:17
 * \* description: 统计一个网站的pv(page view)，uv(unique visitor),同一个ip在一个时间端点击一个网址有多次情况下只算一次， ip去重
 *                  id去重， uv去重
 *                  统计每个网址每个地区的访问量是多少
 * \*/
object ProducePvAndUvData {
  val IP = 223
  val ADDRESS = Array(
    "河北","山西", "辽宁", "吉林", "黑龙江", "江苏",
    "浙江", "安徽", "福建", "江西", "山东", "台湾",
    "河南", "湖北", "湖南", "广东", "海南", "四川",
    "贵州", "云南", "陕西", "甘肃", "青海", "北京",
    "上海", "重庆", "天津", "深圳"
  )
  val DATE = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  val TIMESTAMP = 0L
  val USERID = 0L
  val WEBSITE = Array("www.baidu.com", "www.taobao.com", "www.dangdang.com",
                    "www.jd.com", "www.suning.com", "www.huawei,com")
  val ACTION = Array("Regist", "Comment", "View", "Login", "Buy", "Click", "Logout")

  def main(args: Array[String]): Unit = {
    val pathFileName = "D:\\BigDataProj\\Spark\\SparkWC\\data\\pvuvdata"
    val createFile = CreateFile(pathFileName)
    //val file = new File(pathFileName)
    val file = new File(pathFileName)
    val fos = new FileOutputStream(file, true)
    val osw = new OutputStreamWriter(fos, "UTF-8")
    val pw = new PrintWriter(osw)
    if (createFile){
      var i = 0
      while (i < 50000){
        val random = new Random()
        val ip = random.nextInt(IP) + "." + random.nextInt(IP) + "." + random.nextInt(IP) + "." + random.nextInt(IP)
        val address = ADDRESS(random.nextInt(28))
        val date = DATE
        val userid = Math.abs(random.nextLong())

        /**
         * 这里的while模拟的是同一个用户不同时间点对不同网站的操作
         */
        var j = 0
        var timestamp = 0L
        var webSite = "未知网站"
        var action = "未知行为"
        val flag = random.nextInt(5) | 1
        while (j < flag){
          timestamp = new Date().getTime()
          webSite = WEBSITE(random.nextInt(6))
          action = ACTION(random.nextInt(6))
          j += 1

          /**
           * 拼装
           */
          val content = ip + "\t" + address + "\t" + date + "\t" + timestamp + "\t" + userid + "\t" + webSite + "\t" +action
          System.out.println(content)
          pw.write(content + "\n")
        }
        i += 1
      }
      pw.close()
      osw.close()
      fos.close()
    }

  }

  def CreateFile(pathFileName:String):Boolean = {
    val file: File = new File(pathFileName)
    if (file.exists) {file.delete}
    val createNewFile = file.createNewFile()
    println("create file" + pathFileName + " success!")
    createNewFile
  }
}
