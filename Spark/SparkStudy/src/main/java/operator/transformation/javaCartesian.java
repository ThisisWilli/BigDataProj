package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 12:27:59
 * \* Description:cartesian算子，对两个RDD中的每个元素进行笛卡尔积操作
 * \
 */
public class javaCartesian {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaCartesian");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<String> data1 = context.textFile("data/num1");
        JavaRDD<String> data2 = context.textFile("data/num2");
        data1.cartesian(data2).foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });
    }
}