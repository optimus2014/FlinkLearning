package com.learning.flink.demo;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
        // 获取flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，默认使用机器CPU数量
        env.setParallelism(1);
        SourceDemo source = new SourceDemo();
        /**
         * 首先，设置数据流的内嵌时间类型
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // Source,获取原始DataStream
        DataStream<Tuple2<String, Integer>> data = source.getStream(env).flatMap(new Tokenizer());
        data = data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.minutes(10)) {
            @Override
            public long extractTimestamp(Tuple2<String, Integer> element) {
                return element.f1;
            }
        });

        // 设置保存被丢弃的数据
        OutputTag<Tuple2<String,Integer>> outputTag = new OutputTag<Tuple2<String,Integer>>("late-data"){};

        // window执行，做算子转换
        SingleOutputStreamOperator operator = WindowLearningDemo.windowTest1(data,outputTag);

        // window处理后的Dataframe输出到控制台
        operator.print();

        // 获取迟到数据，打印在控制台
        DataStream<Tuple2<String, Integer>> sideOutput = operator.getSideOutput(outputTag);
        sideOutput.print();

        // 程序执行
        env.execute();
    }

    /**
     * 测试Window的各项功能属性
     * @param ds
     */
    public static SingleOutputStreamOperator windowTest1(DataStream<Tuple2<String,Integer>> ds,
                                                  OutputTag<Tuple2<String,Integer>> outputTag){
        SingleOutputStreamOperator operator = ds.keyBy(item -> item.f0)
                // window()函数设置窗口类型，设置WindowAssigner窗口分配类型
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10))) // 设置滚动窗口，窗口分配器，决定数据分配到哪个窗口中
                //设置触发器，窗口触发关闭，注：Trigger的子类构造函数类型是private，需要使用create()函数实现。
                // 每一个类型的窗口都有默认的触发器，如果需要的话，可以自定义触发器
                .trigger(EventTimeTrigger.create())
                // 窗口驱逐器，类似于Filter功能，设置2ms之内的数据剔除掉
                .evictor(TimeEvictor.of(Time.milliseconds(2)))
                // 设置时间窗口允许的延迟，Event Time 的Watermark
                .allowedLateness(Time.milliseconds(50))  // 允许数据迟到2s
                // 延迟的数据，分流道那个“流”中
                .sideOutputLateData(outputTag)  // 设置迟到数据处理逻辑
                .sum(1);
        return operator;
    }

    /**
     * Watermark Test
     */
}
