package com.satan.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/09/11
 */
public class CustomProducer extends ProducerBase {

    private static final String topic = "bigdata";

    public static void main(String[] args) {
        Properties init = init();
        KafkaProducer<String, String> producer = new KafkaProducer<>(init);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "aaaaa" + i);
            producer.send(record, (recordMetadata, e) -> {
                if (Objects.isNull(e)) {
                    System.out.println("发送成功" + recordMetadata.toString());
                }
            });
        }
        producer.close();
    }
}
