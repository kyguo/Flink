package com.didichuxing.restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateBackEndSaveDemo {
    public static void main(String[] args) throws Exception {
        //设置访问hdfs集群的账号
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //只有开启checkpoint，才会有重启策略。设定5秒做一次checkpoint。当任务挂了时，会找挂之前最近的一次checkpoint，如果最近的一次checkpoint之后还有数据输入，则这部分数据会丢弃
        //如设定checkpoint时间为10秒，输入 flink ——> 等待10秒(checkpoint) ——>（10秒内）再快速输入 flink  laoduan ——>重启 ——>输入flink，则输出(flink,2)，中间的那个flink丢了，因为程序挂掉前最近一次checpoint是在中间的flink之前
        env.enableCheckpointing(5000);
        //设定重启次数是无限重启，两次重启之间的延迟是（至少）2秒(比如没有开nc -lk 8888，则任务会连续重启三次，每次重启间隔2秒。如果是那种重启后运行了一段时候再挂，其实这里的2秒没有意义)
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2000));

        //设置backend  'file://' 或 'hdfs://'
        env.setStateBackend(new FsStateBackend("file:////Users/didi/Desktop/flink-java"));
        //需要引入hadoop-hdfs包，直接引入hadoop-client也可以，因为hadoop-client包含了hadoop-hdfs包
//        env.setStateBackend(new FsStateBackend("hdfs:////Users/didi/Desktop/flink-java"));

        //任务取消时或达到重启次数退出后，保留BackEnd文件
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.startsWith("laoduan")) {
                    throw new RuntimeException("老段来了，程序挂了");
                }
                return Tuple2.of(value, 1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = wordAndOne.keyBy(0).sum(1);
        res.print();
        env.execute("StateBackEndSaveDemo");

    }
}
