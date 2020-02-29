package com.didichuxing.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TumblingWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> value = lines.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });
        //不分组，把整体当做一个组
        AllWindowedStream<Integer, TimeWindow> window = value.timeWindowAll(Time.seconds(5));
        SingleOutputStreamOperator<Integer> res = window.sum(0);
        res.print();
        env.execute("WindowCountAll");
    }
}
