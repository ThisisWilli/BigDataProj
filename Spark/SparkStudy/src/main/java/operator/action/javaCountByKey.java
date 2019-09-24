package operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * \* project: SparkStudy
 * \* package: operator.action
 * \* author: Willi Wei
 * \* date: 2019-09-24 10:28:20
 * \* description:统计数据中每个key的出现次数
 * \
 */
public class javaCountByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaCountByKey");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaPairRDD<String, Integer> rdd1 = context.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 10),
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("lisi", 10),
                new Tuple2<String, Integer>("lisi", 20),
                new Tuple2<String, Integer>("wangwu", 10)
        ));
        Map<String, Long> result = rdd1.countByKey();
        for (Map.Entry<String, Long> entry : result.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }
}