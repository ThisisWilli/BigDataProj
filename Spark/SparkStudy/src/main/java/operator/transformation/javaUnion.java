package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 21:35:34
 * \* Description:
 * \
 */
public class javaUnion {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaUnion");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setLogLevel("Error");
        JavaRDD<String> data1 = javaSparkContext.textFile("data/num1");
        JavaRDD<String> data2 = javaSparkContext.textFile("data/num2");
        data1.union(data2).foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println( s + "by union");
            }
        });
    }
}