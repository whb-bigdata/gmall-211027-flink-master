package com.atguigu.app;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Objects;
import java.util.Properties;

public class KafkaConsumerTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_211027");
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("test",
                new SimpleStringSchema(),
                properties));

        SingleOutputStreamOperator<String> filter = streamSource.filter(Objects::nonNull);

        filter.print();

        env.execute();

    }

}
