package com.didichuxing.AsyncActivityLocation;

import com.didichuxing.ActivityLocation.ActivityLocatonBean;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class AsyncLatLngToLocation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<ActivityLocatonBean> res = AsyncDataStream.unorderedWait(lines, new AsyncAccessMapFunction(), 1000, TimeUnit.MILLISECONDS, 10);
        res.print();
        env.execute("AsyncLatLngToLocation");
    }
}
