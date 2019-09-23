package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 12:58:24
 * \* Description:改变数据的分组个数 ,index和value
 * \
 */
public class javaRepartition {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaRepartition");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<String> data1 = context.parallelize(new ArrayList<String>(Arrays.asList("love1","love2","love3","love4",
                "love5","love6","love7","love8", "love9","love10","love11","love12", "love13","love14","love15","love16")) {}, 3);
        JavaRDD<String> result1 = data1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                LinkedList<String> linkedList = new LinkedList<String>();
                while (stringIterator.hasNext()){
                    linkedList.add(Integer.toString(integer) + "---" + stringIterator.next());
                }
                return linkedList.iterator();
            }
        }, false);
        result1.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        System.out.println("repartition后................");
        JavaRDD<String> result2 = result1.repartition(5);
        result2.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}