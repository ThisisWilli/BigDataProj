package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 13:26:12
 * \* description:distinct算子，对RDD中的数据进行去重操作
 * \
 */
public class javaDistinct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaDistinct");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 1, 2, 2, 3, 4, 3, 3, 9, 10, 11, 12), 3);
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            public Iterator<String> call(Integer integer, Iterator<Integer> iter) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (iter.hasNext()){
                    list.add(integer.toString() + "--->" + iter.next().toString());
                }
                return list.iterator();
            }
        }, false).foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        rdd.distinct().foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}