package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 13:07:01
 * \* description:coalesce算子，与repartition相似
 * \
 */
public class javaCoalesce {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaCoalesce");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 3);
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            public Iterator<String> call(Integer integer, Iterator<Integer> iter) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (iter.hasNext()){
                    String currOne = integer.toString();
                    list.add("rdd partition index = ["+integer+"],value=["+currOne+"]");
                }
                return list.iterator();
            }
        }, false).foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        System.out.println("caolesce之后........................");
        JavaRDD<Integer> result = rdd.coalesce(4);
        result.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            public Iterator<String> call(Integer integer, Iterator<Integer> iter) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (iter.hasNext()){
                    String currOne = integer.toString();
                    list.add("rdd partition index = ["+integer+"],value=["+currOne+"]");
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