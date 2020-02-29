package com.didichuxing.demo.mysqlConnect.stream;

import com.didichuxing.FlinkUtils.FlinkUtils;
import com.didichuxing.demo.mysqlConnect.stream.ActivityBean;
import com.didichuxing.demo.mysqlConnect.stream.NameAccessMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


/**
 * 模块1：FlinkUtils/FlinkUtils.java封装了消费kafka的模块
 * 模块2：自定义了MapMunction，在MapFunction中输出的对象要求是一个POJO，所以又定义了ActivityBean模块
 */

public class QueryActicityName {
    public static void main(String[] args) throws Exception {
        DataStream<String> lines = FlinkUtils.createKafkaStream(args, new SimpleStringSchema());
        SingleOutputStreamOperator<ActivityBean> res = lines.map(new NameAccessMapFunction());
        res.print();
        FlinkUtils.getEnv().execute("QueryActicityName");

    }
}
