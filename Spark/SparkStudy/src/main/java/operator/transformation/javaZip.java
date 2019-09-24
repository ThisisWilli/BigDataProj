package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 10:25:41
 * \* description:
 * \
 */
public class javaZip {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaZip");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<String> data1 = context.parallelize(new ArrayList<String>(Arrays.asList("love1","love2","love3","love4",
                "love5","love6","love7","love8", "love9","love10","love11","love12", "love13","love14","love15","love16")) {}, 3);
        JavaPairRDD<String, Long> data2 = data1.zipWithIndex();
        data2.foreach(new VoidFunction<Tuple2<String, Long>>() {
            public void call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                System.out.println(stringLongTuple2);
            }
        });
    }

}