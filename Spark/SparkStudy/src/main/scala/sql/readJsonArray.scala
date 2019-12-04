package sql

import org.apache.spark.sql.SparkSession

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-01 20:30:55
 * \* description: 
 * \*/
object readJsonArray {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("readNestJsonFile").getOrCreate()

    // 读取嵌套的json文件
    val frame = spark.read.format("json").load("data/jsonArrayFIle")
    // 不折叠显示
    frame.show(false)
    frame.printSchema()

    // 导入explode
    import org.apache.spark.sql.functions._
    // 导入这个可以使用$符
    import spark.implicits._
    // explode负责将json格式的数组展开，数组中每个json对象都是一条数据
    val transDF = frame.select($"name", $"age", explode($"scores")).toDF("name", "age", "allScores")
    transDF.show(100, false)
    transDF.printSchema()
  }
}
