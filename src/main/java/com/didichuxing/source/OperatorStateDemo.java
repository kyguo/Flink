package com.didichuxing.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class OperatorStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        DataStreamSource<Tuple2<String, String>> lines = env.addSource(new ParallelizeSourceFunction("/Users/didi/Desktop/para"));
        lines.print();
        DataStreamSource<String> lineSocket = env.socketTextStream("localhost", 8888);
        lineSocket.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if(value.startsWith("laoduan")) {
                    throw new RuntimeException("老段来了");
                }
                return  value;
            }
        }).print();
        env.execute();
    }

}
