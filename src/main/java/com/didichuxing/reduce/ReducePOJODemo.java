package com.didichuxing.reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**目的：类似spark的reduce功能，对输入的数据进行聚合操作，前一条数据和后一条数据进行聚合
 **注意：输入和输出数据类型一定要一致，本案例中输入B类型，输出表类型
 * 输入：spark 1
 *      spark 2
 *      hadoop 3
 *      hadoop 4
 *      spark 5
 * 输出：1> B( spark, 1 )
 *      1> B( spark, 3 )
 *      4> B( hadoop, 3 )
 *      4> B( hadoop, 7 )
 *      1> B( spark, 8 )
 */

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
