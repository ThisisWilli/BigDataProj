package streaming.kafka.bean;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Deserializer;


import java.util.Map;

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka.bean
 * \* author: Willi Wei
 * \* date: 2019-12-23 15:13:53
 * \* description:
 * \
 */
public class PersonDeserializer implements Deserializer<Person> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public Person deserialize(String topic, byte[] data) {
        return JSON.parseObject(data, Person.class);
    }

    public void close() {

    }
}