package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.actors.threadpool.Arrays;

import java.util.Iterator;
import java.util.List;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 20:12:15
 * \* Description:
 * \
 */
public class javaFlatMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaFlatMap");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setLogLevel("Error");
        JavaRDD<String> lines = javaSparkContext.textFile("data/data.txt");
        lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> list = Arrays.asList(s.split(" "));
                return list.iterator();
            }
        }).foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}