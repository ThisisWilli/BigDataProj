package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-22 20:21:52
 * \* Description:
 * \
 */
public class javaMapPartitions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaMapPartitions");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setLogLevel("Error");
        JavaRDD<String> mapPartitions = javaSparkContext.textFile("data/num1");
        mapPartitions.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            public Iterator<Integer> call(Iterator<String> iter) throws Exception {
                // 创建要返回的list
                List<Integer> result = new ArrayList<Integer>();
                while (iter.hasNext()){
                    String next = iter.next();
                    Integer num = Integer.parseInt(next);
                    result.add(num);
                }
                return result.iterator();
            }
        }).foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}