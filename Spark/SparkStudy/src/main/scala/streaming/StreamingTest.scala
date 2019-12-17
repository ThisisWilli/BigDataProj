package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * \* project: SparkStudy
 * \* package: streaming
 * \* author: Willi Wei
 * \* date: 2019-12-09 15:54:26
 * \* description: 
 * \*/
object StreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("streamingTest")

//    创建ssc的两种方式
    val ssc = new StreamingContext(conf, Durations.seconds(5))
//    val ssc = new StreamingContext(sc, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node04", 9999)
    val words = lines.flatMap(one=>{one.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(one=>{(one, 1)})
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int)=>{v1 + v2})
    result.print(100)

    ssc.start()
    ssc.awaitTermination()

    ssc.stop()
  }
}
