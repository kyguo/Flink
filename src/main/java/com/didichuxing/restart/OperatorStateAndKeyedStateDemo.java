package com.didichuxing.restart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 目的：用来观察OperatorState和KeyedState，代码中有4个必须设置的条件
 * OperatorState为kyed之前的state，是消费者用来记录消费数据时的偏移量，消费者对应subtask
 * KeyedState为keyBy之后，进行聚合操作，进行历史数据累加，这些subtask使用分组累加后的历史就是keyedState
 * 本demo中需要记录的state有两大类，OperatorState 和 KeyedState，这两类state其实不需要用户自己通过继承一些类去实现，只要把相应配置信息补充即可：开启CheckPoint + 关闭kafka消费者提交偏移量，其余的诸如BackEnd位置（JobManager还是hdfs）、CheckPoint模式（ExactlyOnce还是AtLeastOnce）都是可选的
 */
public class OperatorStateAndKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //（1）必须设置：开启flink CheckPonit机制
        env.enableCheckpointing(5000);
        //下面这段设置重启策略的代码可以不用，因为默认是无限重启
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        //（2）设置CheckPoint机制为EXACTLY_ONCE，必须记录偏移量
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        //（3）必须设置：任务取消时或达到重启次数退出后，保留BackEnd文件
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置backend  'file://' 或 'hdfs://'
        env.setStateBackend(new FsStateBackend("file:////Users/didi/Desktop/flink-java"));
        //需要引入hadoop-hdfs包，直接引入hadoop-client也可以，因为hadoop-client包含了hadoop-hdfs包
//        env.setStateBackend(new FsStateBackend("hdfs:////Users/didi/Desktop/flink-java"));

        Properties properties = new Properties();
        //kafka broker地址
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // zk地址，only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        //如果不指定auto.offset.reset 为earliest，则只会读任务启动之后的消息，之前的消息都不会读；反之，会去读记录在kafka 特定topic[__consumer_offsets] 里面的，本消费者的最新偏移量之后的数据
        //earliest不是说kafka最早的数据，而是指【当前消费者】所消费的最新偏移量之后的数据（已经消费过的就不会再去消费了）
        properties.setProperty("auto.offset.reset", "earliest");

        //（4）必须设置：kafka消费者不主动提交偏移量，而是交给flink通过checkpoint管理偏移量
        properties.setProperty("enable.auto.commit", "false");


        //SimpleStringSchema以字符串的形式进行序列化和反序列化
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), properties);
        DataStream<String> lines = env.addSource(kafkaSource);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(" ");
                for (String field : fields) {
                    out.collect(Tuple2.of(field, 1));
                }

            }
        });
        //为了保证程序出现问题可以继续累加，要记录分组聚合后的中间结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = wordAndOne.keyBy(0).sum(1);
        res.print();
        env.execute("OperatorStateAndKeyedStateDemo");

    }
}
