package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 10:20:27
 * \* Description:
 * \
 */
public class javaMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("map");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setLogLevel("Error");
        JavaRDD<String> lines = javaSparkContext.textFile("data/data.txt");
        JavaRDD<String> map = lines.map(new Function<String, String>() {
            public String call(String line) throws Exception {
                return line + "*";
            }
        });
        map.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}