package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-26 14:33:18
 * \* description:
 * \
 */
public class javaReduceByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaReduceByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(new ArrayList<Tuple2<String, Integer>>(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("zhangsan", 250),
                new Tuple2<String, Integer>("lisi", 1000),
                new Tuple2<String, Integer>("lisi", 2500)
        )));
        JavaPairRDD<String, Integer> result = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer * integer2;
            }
        });
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + "   " + tuple2._2);
            }
        });
    }
}