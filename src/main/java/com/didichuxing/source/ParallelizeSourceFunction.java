package com.didichuxing.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;

/**
 * 目的：实现自定义并行source
 * initializeState：初始化，在每个SubTask的实例里最开始会执行这个方法。要记录历史OperatorState（偏移量）
 * run：主函数，一般会在里面套一个while循环。里面要实现：
 *      （1）从历史的OperatorState中获取offset，并从该偏移量出开始读
 *      （2）读取文件并发送出去
 *      （3）更新偏移量，注意这里要加锁（因为snapshotState是做offet的备份）
 *snapshotState：做（偏移量）备份
 *cancel：取消读取，执行cancel方法后，run方法会停止（通过改变成员变量的值，让run中的while循环条件不成立）
 */

public class ParallelizeSourceFunction extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private String path;

    private boolean flag = true;

    private long offset = 0;

    //程序中出现的状态值最好transient
    private transient ListState<Long> offsetState;

    public ParallelizeSourceFunction() {
    }


    public ParallelizeSourceFunction(String path) {
        this.path = path;
    }



    /**
     * 初始化OperatorState，在每个SubTask的实例里最开始会执行这个方法
     */

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //定义状态描述器，虽然是ListStateDescriptor<Long> ，但是一个subtask里面其实只有一个值
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<Long>(
                        "offset-state", // the state name，在StateBackEnd的文件内容中会和其他state name作区分
                        TypeInformation.of(new TypeHint<Long>() {}
                        )
                );
        //从历史数据汇总恢复
        offsetState = context.getOperatorStateStore().getListState(descriptor);

    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        Iterator<Long> iterator = offsetState.get().iterator();
        if (iterator.hasNext()){
            offset = iterator.next();
        }

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();


        RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt","r");

        //从固定的文件偏移处开始读
        randomAccessFile.seek(offset);

        while(flag){
            String line = randomAccessFile.readLine();
            if(line != null){
                //防止和snapshotState发生线程不安全
                synchronized (ctx.getCheckpointLock()){
                    String res = new String(line.getBytes("ISO-8859-1"), "utf-8");
                    //返回从文本读出来的消息
                    ctx.collect(Tuple2.of(String.valueOf(indexOfThisSubtask),res));
                    offset = randomAccessFile.getFilePointer();
                }

            }else {
                Thread.sleep(1000);
            }

        }

    }

    //将历史状态值清除，并更新最新的状态值
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.update(Collections.singletonList(offset));

    }

    @Override
    public void cancel() {

        flag =false;

    }
}
