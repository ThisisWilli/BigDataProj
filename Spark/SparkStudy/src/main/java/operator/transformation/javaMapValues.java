package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-25 13:30:58
 * \* description: mapvalue算子，针对k-v数据中的value进行map，而不对key进行处理
 * \
 */
public class javaMapValues {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaMapValues");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
//        List<Tuple2<String, Integer>> test = new ArrayList<Tuple2<String, Integer>>;
//        test.add(new Tuple2<String, Integer>("zhangsan", 3));
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(new ArrayList<Tuple2<String, Integer>>(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 300),
                new Tuple2<String, Integer>("zhangsan", 200),
                new Tuple2<String, Integer>("lisi", 100),
                new Tuple2<String, Integer>("lisi", 400)
        )
        ));
        // 第一个Integer对应传入的参数，第二个Integer对应返回的参数
        rdd.mapValues(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer + 250;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + tuple2._2);
            }
        });
    }
}