package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-04 09:37:53
 * \* description:
 * \
 */

/**
 * 读取json格式的文件创建DF
 */
public class javaReadNestJsonFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("readNestJsonFile");
                SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        Dataset<Row> frame = spark.read().json("data/NestJsonFile");
        frame.show();
        frame.printSchema();
        frame.createOrReplaceTempView("infoview");
        spark.sql("select name, score, infos.age, infos.gender from infoview").show();
    }
}