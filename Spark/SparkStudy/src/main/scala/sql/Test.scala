package sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-11-29 13:52:30
 * \* description:
 * \*/
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val df = spark.read.json("data/json")
    df.show()
    df.printSchema()
    val rows = df.take(10)
    rows.foreach(println)
  }
}
