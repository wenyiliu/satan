package com.satan.flink.custom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author liuwenyi
 * @date 2022/9/28
 **/
public class StateFulMapFunc extends RichMapFunction<String, String> implements CheckpointedFunction {

    private ListState<String> listState;

    @Override
    public String map(String value) throws Exception {
        return null;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("", String.class));
    }
}
