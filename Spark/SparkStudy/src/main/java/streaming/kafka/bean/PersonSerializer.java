package streaming.kafka.bean;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka.bean
 * \* author: Willi Wei
 * \* date: 2019-12-23 15:22:43
 * \* description:
 * \
 */
public class PersonSerializer implements Serializer<Person> {
    public void configure(Map map, boolean b) {

    }

    public byte[] serialize(String topic, Person person) {
        return JSON.toJSONBytes(person);
    }

    public void close() {

    }
}