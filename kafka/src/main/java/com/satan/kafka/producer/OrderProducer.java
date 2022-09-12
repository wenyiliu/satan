package com.satan.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.satan.kafka.entity.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

/**
 * @author liuwenyi
 * @date 2022/09/11
 */
public class OrderProducer extends ProducerBase {

    private static final List<String> nameList = Lists.newArrayList("A", "B", "C", "D", "E", "F", "G", "H",
            "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");

    private static String getOrderValue() {
        Random random = new Random();
        long currentTimeMillis = System.currentTimeMillis();
        long id = (currentTimeMillis - 1662860000000L);
        Order order = Order.builder()
                .id((int) id)
                .timestamp(currentTimeMillis)
                .amount(BigDecimal.valueOf(random.nextDouble() * 1000))
                .type(random.nextInt(10))
                .name(nameList.get(random.nextInt(25)))
                .build();
        return JSON.toJSONString(order);
    }

    public static void order(Integer num, String topic) throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(init());
        while (num > 0) {
            String orderValue = getOrderValue();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, orderValue);
            producer.send(record);
            System.out.println(orderValue);
            num--;
            Thread.sleep(1);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // ./kafka-topics.sh --zookeeper hadoop01:2181,hadoop02:2181,hadoop03:2181/kafka --create --topic order --partitions 3 --replication-factor 2
        String topic = "order";
        Integer num = 10000000;
        order(num, topic);
//        System.out.println((System.currentTimeMillis() - 1662860000000L));
    }
}
