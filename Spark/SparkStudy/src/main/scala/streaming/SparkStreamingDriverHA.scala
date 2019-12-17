package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * \* project: SparkStudy
 * \* package: streaming
 * \* author: Willi Wei
 * \* date: 2019-12-17 15:35:06
 * \* description: 
 * \*/
/**
 * Driver HA
 * 1.在提交application的时候，添加--supervise选项，如果Driver挂掉，会自动启动一个Driver
 * 2.代码层面恢复Driver(StreamingContext)，就是恢复之前Driver中的代码逻辑
 */
object SparkStreamingDriverHA {
  val ckDir = "data/streamingCheckPoint"
  def main(args: Array[String]): Unit = {
    /**
     * StreamingContext.getOrCreate
     * 这个方法首先会从ckDir目录中获取StreamingContext（因为StreamingContext是序列化存储在Checkpoint目录中，恢复时会尝试反序列化这些object）
     * 如果从ckDir目录中无法恢复，则从第二个参数中的函数中重新创建一个StreamingContext
     * 如果用修改过的class可能会导致错误，此时需要更换checkpoint目录或者删除checkpoint目录中的数据，程序才能起来
     *
     * 若能获取回来StreamingContext，就不会执行CreateStreamingContext这个方法创建，否则就会创建
     */
    val ssc: StreamingContext = StreamingContext.getOrCreate(ckDir, CreateStreamingContext)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def CreateStreamingContext(): Unit ={
    println("================create new StreamingContext===================")
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("DriverHA")
    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")

    /**
     * 默认checkpoints存储
     * 1、配置信息
     * 2、DStream操作逻辑
     * 3、job的执行进度
     * 4、offset,kafka 中的偏移变量
     */

    ssc.checkpoint(ckDir)
    val lines: DStream[String] = ssc.textFileStream("./data/streamingCopyFile")
    val words: DStream[String] = lines.flatMap(line=>{line.trim.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word, 1)})
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int)=>{v1+v2})
    result.print()
    return ssc
  }
}
