package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * \* project: SparkStudy
 * \* package: streaming
 * \* author: Willi Wei
 * \* date: 2019-12-16 10:26:58
 * \* description:
 * \*/
/**
 * SparkStreaming窗口操作
 * reduceByKeyAndWindow
 * 每隔窗口滑动间隔时间计算窗口长度内的数据，按照指定的方式处理，只看一段时间内的数据
 */
object WindowOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("windowOperator")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node04", 9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word, 1)})

    /**
     * 窗口操作的普通机制
     *
     * 滑动间隔和窗口长度必须是batchInterval的整数倍
     */
      // 每隔5s计算过去15s内的数据
//    val windowResult: DStream[(String, Int)] =
//      pairWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>{v1 + v2}, Durations.seconds(15), Durations.seconds(5))

    ssc.checkpoint("data/streamingCheckPoint")
    /**
     * 窗口操作优化的机制，加的是新进来批次的值，减的是之前批次的值
     */
    val windowResult: DStream[(String, Int)] =
      pairWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>{v1 + v2},
                                      (v1:Int, v2:Int)=>{v1-v2},
                                      Durations.seconds(15),
                                      Durations.seconds(5))


    windowResult.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
