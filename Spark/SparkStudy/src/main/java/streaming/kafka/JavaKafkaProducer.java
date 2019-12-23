package streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import streaming.kafka.bean.Person;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka
 * \* author: Willi Wei
 * \* date: 2019-12-22 20:15:26
 * \* description:测试发送序列化的person数据
 * \
 */
public class JavaKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node02:9092,node03:9092,node04:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        // key的序列化方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value的序列化方式
        props.put("value.serializer", "streaming.kafka.bean.PersonSerializer");
        KafkaProducer<String, Person> producer = new KafkaProducer<String, Person>(props);
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/test2000.csv"));
            reader.readLine();//第一行信息，为标题信息，不用，如果需要，注释掉
            String line = null;
            while((line=reader.readLine())!=null){
                //CSV格式文件为逗号分隔符文件，这里根据逗号切分
                String items[] = line.split(",");
                Person person = new Person();
                person.setUserId(items[0]);
                person.setAgeRange(items[1]);
                person.setGender(items[2]);
                person.setMerchantId(items[3]);
                if (items.length == 4)
                {
                    person.setLabel("null");
                }else {
                    person.setLabel(items[4]);
                }
                producer.send(new ProducerRecord<String, Person>("topic1223",person.getUserId(), person));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}