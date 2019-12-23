package streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.streaming.Durations;
import streaming.kafka.bean.Person;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka
 * \* author: Willi Wei
 * \* date: 2019-12-23 15:51:37
 * \* description:
 * \
 */
public class JavaKafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node02:9092,node03:9092,node04:9092");
        props.put("group.id", "group01");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化方式
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化方式
        props.put("value.deserializer", "streaming.kafka.bean.PersonDeserializer");
        KafkaConsumer<String, Person> consumer = new KafkaConsumer<String, Person>(props);
        consumer.subscribe(Arrays.asList("topic1223"));
        try {
            while(true){
                ConsumerRecords<String, Person> records = consumer.poll(5000);
                System.out.println("接收到的信息总数为：" + records.count());
                for (ConsumerRecord<String, Person> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}