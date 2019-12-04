package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-04 09:20:04
 * \* description:
 * \
 */
import org.apache.spark.sql.SparkSession.implicits$;
// 导入这个可以使用$符

public class javaReadJsonArray {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("javaReadJsonArray")
                .config("spark.some.conf.option", "readJson")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("data/jsonArrayFIle");
//        df.show();
//        df.printSchema();
        Column scores = new Column("scores");
        Column name = new Column("name");
        Column age = new Column("age");
        Column allScores = org.apache.spark.sql.functions.explode(scores);
        Dataset<Row> result = df.select(name, age,
                org.apache.spark.sql.functions.explode(scores)).toDF("name", "age", "allScores");
        result.printSchema();
    }
}