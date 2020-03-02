package com.didichuxing.aggregation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 目的：测试每个subtask进行累加的机制：如果利用map()+${aggRes}，只能实现一个subtask内的数据累加，而不能进行每个subtask的每个组的累加
 * 结论：若要实现每个subtask每个组的累加，必须利用ValueState
 * 输入： spark hadoop
 * 输出  1> (spark,1)
 *      4> (hadoop,1)
 * 接着继续
 * 输入  scala  hadoop2.7
 * 输出：1> (spark,2)
 *         4> (hadoop,2)
 * 原因分析：由于spark和scala都归属于资源槽1，也就是都在subtask1里面，共享aggRes变量，因此其实输入scala和spark没啥区别
 *
 */
public class SubtaskSumFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fileds = value.split(" ");
                for (String filed : fileds) {
                    out.collect(Tuple2.of(filed, 1));
                }

            }
        });
//        wordAndOne.keyBy().sum()

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = wordAndOne.keyBy(0).map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

//            private transient ValueState<Tuple2<String,Integer>> state;
            private transient Tuple2<String, Integer> aggRes;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //定义
                aggRes = null;
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                if (aggRes == null) {
                    aggRes = value;
                } else {
                    aggRes.f1 += value.f1;
                }

                return aggRes;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });
        res.print();
        env.execute("SubtaskSumFunction");

    }
}
