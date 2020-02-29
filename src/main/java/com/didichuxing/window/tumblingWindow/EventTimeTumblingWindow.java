package com.didichuxing.window.tumblingWindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class EventTimeTumblingWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        assignTimestampsAndWatermarks只是为了提取时间字段，并不会改变lines的内容（时间字段单位必须是毫秒）
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 8888).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });
//        assignTimestampsAndWatermarks只是为了提取时间字段，并不会改变lines的内容（时间字段单位必须是毫秒）
//        Properties properties = new Properties();
//        //kafkabroker姿势
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        // zk地址，only required for Kafka 0.8
//        properties.setProperty("zookeeper.connect", "localhost:2181");
//        properties.setProperty("group.id", "test");
//        //如果不指定auto.offset.reset 为earliest，则只会读任务启动之后的消息，之前的消息都不会读；反之，会去读记录在kafka 特定topic[__consumer_offsets] 里面的，本消费者的最新偏移量之后的数据
//        //earliest不是说kafka最早的数据，而是指【当前消费者】所消费的最新偏移量之后的数据（已经消费过的就不会再去消费了）
//        properties.setProperty("auto.offset.reset","earliest");
//        //SimpleStringSchema以字符串的形式进行序列化和反序列化
//        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("wordCount10", new SimpleStringSchema(), properties);
//        DataStream<String> lines = env.addSource(kafkaSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
//            @Override
//            public long extractTimestamp(String element) {
//                String[] fields = element.split(",");
//                return Long.parseLong(fields[0]);
//            }
//        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndCount.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = window.sum(1);
        res.print();
        env.execute("EventTimeTumblingWindow");


    }
}
