package sql

import org.apache.spark.sql.SparkSession

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-01 19:54:46
 * \* description: 
 * \*/
object readNestJsonFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("readNestJsonFile").getOrCreate()

    // 读取嵌套的json文件
    val frame = spark.read.format("json").load("data/NestJsonFile")
    frame.printSchema()
    frame.show()

    // 注册一张临时表
    frame.createOrReplaceTempView("infosView")
    spark.sql("select name, infos.age, score, infos.gender from infosView").show(100)
  }
}
