package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * \* project: SparkStudy
 * \* package: streaming
 * \* author: Willi Wei
 * \* date: 2019-12-16 09:54:28
 * \* description:
 * \*/
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("UpdateStateByKey")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    // 设置日志级别
    ssc.sparkContext.setLogLevel("ERROR")
    val lines = ssc.socketTextStream("node04", 9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords = words.map(word=>{(word, 1)})

    /**
     * 根据key更新状态，需要设置checkpoint来保存状态
     * 默认key的状态在内存中有一份，在checkpoint中有一份
     *
     *  多久会将内存中的数据(每一个key所对应的状态)写入到磁盘上一份呢
     *      如果你的batchInterval小于10s，那么10s内将内存中的数据写入到磁盘一份
     *      如果batchInterval大于10s，那么就以batchInterval为准
     *
     *  这样做是为了防止频繁的写HDFS
     */
    ssc.checkpoint("data/streamingCheckPoint")

    /**
     * currentValues:当前批次某个key对应所有的value组成的一个集合
     * preValue：以往批次当前key对应的总状态值
     */
    val result: DStream[(String, Int)] = pairWords.updateStateByKey((currentValues: Seq[Int], preValue: Option[Int]) => {
      var totalValues = 0
      if (!preValue.isEmpty) {
        totalValues += preValue.get
      }
      for (value <- currentValues) {
        totalValues += value
      }
      Option(totalValues)
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
