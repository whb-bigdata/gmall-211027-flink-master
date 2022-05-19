package com.atguigu.app;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        WaterSensor waterSensor = new WaterSensor();

        int id = 1000;
        while (true) {

            waterSensor.setId(id + "");
            waterSensor.setVc(2.5);
            id++;
            waterSensor.setTs(System.currentTimeMillis());
            System.out.println(waterSensor);
            kafkaProducer.send(new ProducerRecord<String, String>("test_211027", JSON.toJSONString(waterSensor)));

            kafkaProducer.flush();

            Thread.sleep(1000);
        }

    }

}
