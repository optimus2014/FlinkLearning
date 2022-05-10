package com.learning.flink.demo;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * Flink 状态练习Demo
 */
public class StatusLearningDemo {
    StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(1))   // TTL必选项
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

}
