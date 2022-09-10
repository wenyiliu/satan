package com.satan.common.kafka;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/9/8
 **/
public class KafkaCustomProducer {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.136.20:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.ACKS_CONFIG, "-1");

        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        for (int i = 1; i < 200; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kb09two", "hello world" + i);
            //(topic,value),指定了队列名和塞进去的值
            //发送消息
            producer.send(producerRecord);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("game over");
        //关闭生产者
        producer.close();
    }
}
