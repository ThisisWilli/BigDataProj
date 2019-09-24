package operator.transformation;

import jdk.nashorn.internal.objects.annotations.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-24 13:35:05
 * \* description:抽样算子
 * \
 */
public class javaSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaDistinct");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 1, 2, 2, 3, 4, 3, 3, 9, 10, 11, 12));
        rdd.sample(true, 0.1, 9).foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}