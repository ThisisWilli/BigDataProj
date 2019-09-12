import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * \* Project: SparkWordCount
 * \* Package: PACKAGE_NAME
 * \* Author: Hoodie_Willi
 * \* Date: 2019-09-11 18:41:50
 * \* Description:
 * \
 */
public class JavaOperate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> lines = sc.textFile("D:\\BigDataProj\\Spark\\SparkWC\\data2.txt");
        /**
         * 按单词降序进行排序
         */
        JavaPairRDD<String, Integer> reduceRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> list = Arrays.asList(s.split(" "));
                return list.iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            // 前两个对kv中的参数，第三个对最后一个的参数
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        reduceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tp) throws Exception {
                System.out.println(tp);
            }
        });
        /**
         * reduceByKey
         */
//        lines.flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String s) throws Exception {
//                List<String> list = Arrays.asList(s.split(" "));
//                return list.iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<String, Integer>(word, 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            // 前两个对kv中的参数，第三个对最后一个的参数
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> tp) throws Exception {
//                System.out.println(tp);
//            }
//        });

        /**
         * mapToPair算子
         */
//        JavaPairRDD<String, String> result = lines.mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) throws Exception {
//                return new Tuple2<String, String>(s, s + "#");
//            }
//        });
//        result.foreach(new VoidFunction<Tuple2<String, String>>() {
//            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                System.out.println(stringStringTuple2);
//            }
//        });
        /**
         * flatmap算子
         */
//        lines.flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String s) throws Exception {
//                List<String>list = Arrays.asList(s.split(" "));
//                return list.iterator();
//            }
//        }).foreach(new VoidFunction<String>() {
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
        /**
         * map算子
         */
//        JavaRDD<String> map = lines.map(new Function<String, String>() {
//            public String call(String line) throws Exception {
//                return line + "*";
//            }
//        });
//        map.foreach(new VoidFunction<String>() {
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
        /**
         * filter算子
         */
//        JavaRDD<String> result = lines.filter(new Function<String, Boolean>() {
//            public Boolean call(String line) throws Exception {
//                return "hello spark".equals(line);
//            }
//        });
//        long count = result.count();
//        System.out.println(count);
//        result.foreach(new VoidFunction<String>() {
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
        sc.stop();
    }
}