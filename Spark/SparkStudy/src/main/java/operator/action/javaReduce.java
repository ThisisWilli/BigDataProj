package operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * \* project: SparkStudy
 * \* package: operator.action
 * \* author: Willi Wei
 * \* date: 2019-09-24 10:28:05
 * \* description:
 * \
 */
public class javaReduce {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaReduce");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(result);
    }
}