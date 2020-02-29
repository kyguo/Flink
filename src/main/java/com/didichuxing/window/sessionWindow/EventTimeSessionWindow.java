package com.didichuxing.window.sessionWindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EventTimeSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //assignTimestampsAndWatermarks只是为了提取时间字段，并不会改变lines的内容（时间字段单位必须是毫秒）
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 8888).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndCount.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = window.sum(1);
        res.print();
        env.execute("EventTimeSessionWindow");


    }
}
