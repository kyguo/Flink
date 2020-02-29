package com.didichuxing.FlinkUtils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkUtils {
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static DataStream<String> createKafkaStream(String[] args, SimpleStringSchema simpleStringSchema) {

//        String topic = args[0];
//        String groupId = args[1];
//        String brokerList = args[2];

        Properties properties = new Properties();
        //kafka broker地址
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // zk地址，only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        //如果不指定auto.offset.reset 为earliest，则只会读任务启动之后的消息，之前的消息都不会读；反之，会去读记录在kafka 特定topic[__consumer_offsets] 里面的，本消费者的最新偏移量之后的数据
        //earliest不是说kafka最早的数据，而是指【当前消费者】所消费的最新偏移量之后的数据（已经消费过的就不会再去消费了）
        properties.setProperty("auto.offset.reset", "earliest");
        //SimpleStringSchema以字符串的形式进行序列化和反序列化
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), properties);
        DataStream<String> lines = env.addSource(kafkaSource);
        return lines;
    }

    public static StreamExecutionEnvironment getEnv(){
        return  env;
    }

}
