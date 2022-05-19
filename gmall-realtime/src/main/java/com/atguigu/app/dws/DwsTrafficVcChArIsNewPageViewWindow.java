package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取3个主题的数据创建三个流
        String page_topic = "dwd_traffic_page_log";
        String uv_topic = "dwd_traffic_unique_visitor_detail";
        String uj_topic = "dwd_traffic_user_jump_detail";
        String groupId = "dws_traffic_vc_ch_ar_isnew_page_view_window_211027";

        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));
        DataStreamSource<String> uvStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uv_topic, groupId));
        DataStreamSource<String> ujStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uj_topic, groupId));

        //TODO 3.将3个流统一数据格式  JavaBean
        //3.1 处理UV数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        //3.2 处理UJ数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));
        });

        //3.3 处理Page数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPvDS = pageStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");

            long sv = 0L;
            if (lastPageId == null) {
                sv = 1;
            }

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    sv,
                    1L,
                    page.getLong("during_time"),
                    0L,
                    jsonObject.getLong("ts"));
        });

        //TODO 4.合并三个流并提取事件时间生成Watermark
        SingleOutputStreamOperator<TrafficPageViewBean> unionDS = trafficPageViewWithPvDS.union(
                trafficPageViewWithUjDS,
                trafficPageViewWithUvDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(13)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 5.分组、开窗、聚合
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getIsNew(),
                        value.getVc());
            }
        });
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //优点：增量聚合  来一条聚合一条,效率高,存储空间占用少
//        windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
//            @Override
//            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
//                return null;
//            }
//        });
        //优点：全量聚合  可以计算前百分比的结果、可以获取窗口信息
//        windowedStream.apply(new WindowFunction<TrafficPageViewBean, Object, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<Object> out) throws Exception {
//                window.getStart();
//                window.getEnd();
//            }
//        });
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {

                //获取数据
                TrafficPageViewBean trafficPageViewBean = input.iterator().next();

                //获取窗口信息
                long start = window.getStart();
                long end = window.getEnd();

                //补充窗口信息
                trafficPageViewBean.setStt(DateFormatUtil.toYmdHms(start));
                trafficPageViewBean.setEdt(DateFormatUtil.toYmdHms(end));

                //输出数据
                out.collect(trafficPageViewBean);
            }
        });

        //TODO 6.将数据写出到ClickHouse
        reduceDS.print(">>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_traffic_channel_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 7.启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }

}
