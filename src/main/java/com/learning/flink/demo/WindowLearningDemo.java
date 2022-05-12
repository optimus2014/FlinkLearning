package com.learning.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口学习Demo
 * 时间窗口:timeWindow
 * 数据量窗口:countWindow
 *
 * 窗口类型：
 * 1.滚动窗口
 * 2.滑动窗口
 * 3.Session窗口
 *
 *
 * countWindow(3)
 */
public class WindowLearningDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
//        env.addSource();
        SourceDemo source = new SourceDemo();
        DataStream data = source.getStream(env);
        DataStream<Tuple2<String, Integer>> counts = data.flatMap(new Tokenizer());
        counts.keyBy(item -> item.f0)
                // countWindow(n)是滚动窗口，countWindow(n,2)：是滑动窗口
                // 返回值：WindowedStream<T, KEY, GlobalWindow>
//                .countWindow(5)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2)))
//                .timeWindow(Time.seconds(5),Time.seconds(2))
                .sum(1)
                .print();
        env.execute();
    }
}
