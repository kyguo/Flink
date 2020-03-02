package com.didichuxing.aggregation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 目的：自定义实现keyBy()后的sum操作，利用KeyedState，实现每个Subtask中每个组的sum
 * 输入：spark hadoop scala java python js matlab php go spark hadoop java spark
 * 输出：2> (python,1)
 *      4> (hadoop,1)
 *      2> (php,1)
 *      4> (hadoop,2)
 *      1> (spark,1)
 *      3> (go,1)
 *      1> (scala,1)
 *      1> (java,1)
 *      1> (js,1)
 *      1> (matlab,1)
 *      1> (spark,2)
 *      1> (java,2)
 *      1> (spark,3)
 */

public class KeyedGroupSumFuntion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(" ");
                for (String field : fields) {
                    out.collect(Tuple2.of(field, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = wordAndOne.keyBy(0).map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Tuple2<String, Integer>> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
                        new ValueStateDescriptor<Tuple2<String, Integer>>(
                                "average", // the state name
                                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                                })); // type information
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Tuple2<String, Integer> historyKV = valueState.value();

                if (historyKV == null) {
                    historyKV = value;
                } else {
                    historyKV.f1 += value.f1;
                }
                valueState.update(historyKV);
                //一定要注意这里不能返回null，所在在本段代码中这里返回null不会报错，但由于这里的返回值还在在flink其他地方被调用，所以会出现空指针异常
                return historyKV;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        res.print();
        env.execute("KeyedGroupSumFuntion");
    }
}
