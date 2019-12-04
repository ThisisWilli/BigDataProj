package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-01 19:14:08
 * \* description:
 * \
 */
public class SQLTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SQLContext
        SQLContext sqlContext = new SQLContext(sc);
        //SparkSession sparkSession = new SparkSession(sc);
        Dataset df = sqlContext.read().json("data/json");
        df.show();
        df.printSchema();

        df.select(df.col("name")).show();
        df.createOrReplaceTempView("t1");
        df.createOrReplaceGlobalTempView("t2");
        Dataset<Row> sql = sqlContext.sql("select * from t1");
        sql.show();

    }
}