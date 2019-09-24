package operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * \* project: SparkStudy
 * \* package: operator.action
 * \* author: Willi Wei
 * \* date: 2019-09-24 11:04:13
 * \* description:CountByValue算子，将原先的数据当作key，将这些数据出现的次数当作value，输出key-value
 * \
 */
public class javaCountByValue {
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
        Map<Tuple2<String, Integer>, Long> result = rdd1.countByValue();
        for (Map.Entry<Tuple2<String, Integer>, Long> entry : result.entrySet()){
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }
}