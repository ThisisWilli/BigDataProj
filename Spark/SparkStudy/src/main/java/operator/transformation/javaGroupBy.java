package operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * \* Project: SparkStudy
 * \* Package: operator.transformation
 * \* Author: Willi Wei
 * \* Date: 2019-09-23 12:49:32
 * \* Description:将元素通过函数生成key，将数据转化为key-value格式
 * \
 */
public class javaGroupBy {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("javaGroupBy");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("Error");
        JavaRDD<String> data1 = context.textFile("data/key_value");
        // 要先将数据中的元素分离成元组，再进行groupby
        data1.map(new Function<String, Tuple2<String, String>>() {
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[0], s.split(" ")[1]);
            }
        }).foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });
    }
}