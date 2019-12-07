package sql

import org.apache.spark.sql.SparkSession

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-06 09:59:08
 * \* description: 
 * \*/
object UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("UDAF").getOrCreate()
    val nameList = List[String]("zhangsan", "lisi", "wangwu", "zhangsan", "lisi", "wangwu")
    import spark.implicits._
    val frame = nameList.toDF("name")
    frame.createOrReplaceTempView("students")

    /**
     * 创建UDAF函数
     */
    spark.udf.register("NAMECOUNT", new MyUDAF())
    spark.sql("select name, NAMECOUNT(name) as count from students group by name").show()
  }
}
