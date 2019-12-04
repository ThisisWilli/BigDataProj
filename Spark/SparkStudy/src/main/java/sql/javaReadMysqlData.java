package sql;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-04 15:40:53
 * \* description:
 * \
 */
public class javaReadMysqlData {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("javaReadMysqlData")
                .getOrCreate();
        DataFrameReader reader = spark.read().format("jdbc");
        reader.option("url", "jdbc:mysql://localhost:3306/mydb");
        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "weizefeng");
        reader.option("dbtable", "users");
        Dataset<Row> users = reader.load();
        users.show();
    }
}