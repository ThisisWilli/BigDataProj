package streaming.kafka;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka
 * \* author: Willi Wei
 * \* date: 2019-12-24 15:00:13
 * \* description:官方文档：http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 * \
 */

public class SparkStreamingOnKafkaDirected {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamingOnKafkaDirected");
     JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

}