package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 13:31:05
 * \* Description:
 * \
 */
public class javaMapPartitionWithIndex {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaMapPartitionWithIndex");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<String> data1 = context.parallelize(new ArrayList<String>(Arrays.asList("love1","love2","love3","love4",
                "love5","love6","love7","love8", "love9","love10","love11","love12", "love13","love14","love15","love16")) {}, 10);
        data1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List list = new ArrayList();
                while (stringIterator.hasNext()){
                    list.add(Integer.toString(integer) + "---" + stringIterator.next());
                }
                return list.iterator();
            }
        }, false).foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}