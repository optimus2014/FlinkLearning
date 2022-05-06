package com.learning.flink.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

/***
 * 练习RichFunction
 * 练习Status的增删查等操作
 */
public class RichFunctionStatusDemo {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
//        env.addSource();
        SourceDemo source = new SourceDemo();
        DataStream data = source.getStream(env);
        // 按照空格切分字符串
        data.flatMap(new SplitString())
                // 转化为KeyStream类型
                .keyBy(item -> item)
                // 操作Event类型的状态，做flatMap操作
                .flatMap(new DedupliStatus())
                .print();

        env.execute();
    }


    /**
     * 知识点：内部类可以使用static修饰
     */
    public static class DedupliStatus extends RichFlatMapFunction<String, String> {

        ValueState<Boolean> keyHasBeenSeen;

        @Override
        public void open(Configuration conf){
            ValueStateDescriptor<Boolean> desc =  new ValueStateDescriptor<Boolean>("keyHasBeenSeen", Types.BOOLEAN);
            keyHasBeenSeen = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(String event, Collector<String> collector) throws Exception {
            // 当元素不存在时候，才会做聚合操作，并把当前key值的状态置为true

            if(keyHasBeenSeen.value() == null){
                collector.collect(event);
                keyHasBeenSeen.update(true);
            }
        }
    }



}

/**
 * 切分每一行的数据，剔除停用词
 * */
class SplitString implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        // normalize and split the line,,\W+
        String[] tokens = s.toLowerCase().split("\\W+");
        ArrayList<String> none_list = new ArrayList<>(
                Arrays.asList("the", "of", "so","as","a","it","is","to","on","and","by","in","our"));

        // emit the pairs
        for (String token : tokens) {
            if( none_list.contains(token) || StringUtils.isNumeric(token)){
                continue;
            }
            if (token.length() > 0) {
                collector.collect(token);
            }
        }
    }
}


