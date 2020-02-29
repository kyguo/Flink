package com.didichuxing.ActivityLocation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LatLngToLoactionMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<ActivityLocatonBean> res = lines.map(new GaoDeAccessMapFuntion());
        res.print();
        env.execute("LatLngToLoactionMain");
    }
}
