package streaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka
 * \* author: Willi Wei
 * \* date: 2019-12-22 14:48:41
 * \* description: 
 * \*/
// kafka生产者详解：https://juejin.im/post/5dcd59abf265da0be53e9db3
object ProducerDemo {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers", "node02:9092,node03:9092,node04:9092")
    //    //prop.put("acks", "0")
    prop.put("acks", "all")
    // 请求失败重试次数
    //prop.put("retries", "3")
    // 指定key的序列化方式, key是用于存放数据对应的offset
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 配置超时时间
    prop.put("request.timeout.ms", "60000")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)
    // 测试发送一条简单的消息
    for (i <-0 to(10)){
      producer.send(new ProducerRecord[String, String]("SparkStreaming-Kafka-1222", 0, "python", "斯巴克"))
    }
    // 如果不关闭producer，消息发不出去。。。
    producer.close()
  }
}
