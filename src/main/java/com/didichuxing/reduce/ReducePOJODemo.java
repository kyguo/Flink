package com.didichuxing.reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReducePOJODemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<B> mapB = lines.map(new MapFunction<String, B>() {
            @Override
            public B map(String value) throws Exception {
                String[] words = value.split(" ");
                return new B(words[0], Integer.valueOf(words[1]));
            }
        });
        SingleOutputStreamOperator<B> res = mapB.keyBy("word").reduce(new ReduceFunction<B>() {
            @Override
            public B reduce(B value1, B value2) throws Exception {
                value1.counts += value2.counts;
                return value1;
            }
        });
        res.print();
        env.execute("ReduceDemo");
    }
}
