package com.atguigu.app;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.MyTrigger;
import com.atguigu.bean.WaterSensor;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WmTest03 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费数据 1001,23.5,1234
        DataStreamSource<String> stringDataStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer("test_211027", "test_211027"));

        //转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(line -> JSON.parseObject(line, WaterSensor.class));

        //提取时间戳
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //分组开窗聚合
        SingleOutputStreamOperator<WaterSensor> result = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new MyTrigger())
                .sum("vc");

        //打印并启动
        result.print();
        env.execute();

    }

}
