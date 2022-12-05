package com.satan.pubsub;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwenyi
 * @date 2022/11/18
 **/
public class PubSub {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.MINUTES)));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(400000);
        env.getCheckpointConfig().setCheckpointTimeout(200000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(6);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend stateBackend = new FsStateBackend("file:////Users/liuwenyi/IdeaProjects/data-flinksql-job/data/ck");
        env.setStateBackend(stateBackend);
        DeserializationSchema<JSONObject> messageout = new DeserializationSchema<JSONObject>() {
            private static final long serialVersionUID = 1L;

            @Override
            public TypeInformation<JSONObject> getProducedType() {
                return TypeInformation.of(JSONObject.class);
            }

            @Override
            public JSONObject deserialize(byte[] bytes) throws IOException {
                String s = new String(bytes, StandardCharsets.UTF_8);
                return JSON.parseObject(s);
            }

            @Override
            public boolean isEndOfStream(JSONObject userPubsub) {
                return false;
            }
        };
        PubSubSource<JSONObject> pubSubSource = PubSubSource.newBuilder().withDeserializationSchema(messageout)
                .withProjectName("bitmart-dev")
                .withSubscriptionName("bitmart-userinfo-collect-topic-sub")
                .withPubSubSubscriberFactory(10, Duration.ofSeconds(5), 2)
                .build();
        env.addSource(pubSubSource).print();
        env.execute();
    }
}
