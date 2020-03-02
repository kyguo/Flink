package com.didichuxing.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
/**目的：处理本地/hdfs文件，进行WordCount
 * 输入文件数据为：spark,hadoop
 *              scala,java
 *              go,python
 *              spark,hadoop
 *              php,php
 * 输出文件有四个（对应四个subtask），内容如下
 *              (go,1)
 *              (php,3)
 *              (hadoop,2)
 *              (java,1)
 *              (scala,1)
 *              (python,1)
 *              (spark,2)
 *
 *
 */

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> lines = env.readTextFile("/Users/didi/Desktop/flink-scala/target/test_data.txt");
        DataSet<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        DataSet<Tuple2<String, Integer>> sumed = wordAndOne.groupBy(0).sum(1);
        sumed.writeAsText("/Users/didi/Desktop/flink-scala/target/test_data_out", FileSystem.WriteMode.OVERWRITE);
        env.execute("BatchWordCount");

    }
}
