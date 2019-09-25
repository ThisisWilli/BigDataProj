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
import java.util.Iterator;
import java.util.List;

/**
 * \* project: SparkStudy
 * \* package: operator.transformation
 * \* author: Willi Wei
 * \* date: 2019-09-25 14:18:39
 * \* description: 将RDD中key相同的数据的values收集起来，变为一个新的数据，放入一个新的RDD中，value是一个列表
 * \
 */
public class javaCombineByKey {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local").setAppName("javaCountByKey");
//        JavaSparkContext context = new JavaSparkContext(conf);
//        context.setLogLevel("Error");
//        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
//        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(data);
////转化为pairRDD
//        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
//                return new Tuple2<Integer, Integer>(integer,1);
//            }
//        });
//
//        JavaPairRDD<Integer,String> combineByKeyRDD = javaPairRDD.combineByKey(new Function<Integer, String>() {
//            @Override
//            public String call(Integer v1) throws Exception {
//                return v1 + " :createCombiner: ";
//            }
//        }, new Function2<String, Integer, String>() {
//            @Override
//            public String call(String v1, Integer v2) throws Exception {
//                return v1 + " :mergeValue: " + v2;
//            }
//        }, new Function2<String, String, String>() {
//            @Override
//            public String call(String v1, String v2) throws Exception {
//                return v1 + " :mergeCombiners: " + v2;
//            }
//        });
//        System.out.println("result~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + combineByKeyRDD.collect());
    }
}