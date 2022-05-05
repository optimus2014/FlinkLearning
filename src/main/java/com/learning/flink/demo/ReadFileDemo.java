package com.learning.flink.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.*;    // 这个包引入是必须的
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import sun.util.resources.cldr.chr.TimeZoneNames_chr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadFileDemo {

    static StreamExecutionEnvironment env;
    public static void main(String[] args) throws Exception{
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  //设置默认带上事件时间

        env.setParallelism(2);  // 设置并行度

//        readFromObject();
        readFromFile();
        env.execute();
    }


    public static void readFromObject(){
        /***
         * 读取本地对象
         */
        List<String> test = new ArrayList<String>();
        test.add("AAA");
        test.add("BBB");
        test.add("CCC");
        DataStream df  = env.fromCollection(test);   // DataStream 一经创建就不可改变
        DataStream dff = df.map(new MyMapFunction());
        dff.print();

        df.map(new MapFunction<String,String>() {
            @Override
            public String map(String s) throws Exception {
                return "" + s.charAt(0);
            }
        }).print();
    }

    public static void readFromFile(){
        /**
         * 读取本地文件
        * */
        DataStream file_DF = env.readTextFile("input_path/input_text.txt");
        DataStream<Tuple2<String, Integer>> counts = file_DF.flatMap(new Tokenizer());
        // group by the tuple field "0" and sum up tuple field "1"
        // 按照key值进行汇聚
        counts = counts.keyBy(item -> item.f0)  // 根据传参的f0(第0个元素)做聚合，value是自定义变量
//                .window(countWindows)
                .countWindow(5,2)    // 滑动窗口，一个窗口最多容纳4个
                .sum(1)
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                // 剔除停用词
                ArrayList<String> none_list = new ArrayList<>(
                        Arrays.asList("the", "of", "a","is","to","on","and","by","in","our"));
                if( none_list.contains(stringIntegerTuple2.f0 )){
                    return false;
                }
                // 过滤出现2次以下的单词
                if(stringIntegerTuple2.f1 <= 1)
                    return false;
                else
                    return true;
            }
        });
        counts.print();

    }
}

class MyMapFunction implements MapFunction<String,String>{

    @Override
    public String map(String s) throws Exception {
//        System.out.println(s.concat(s));
        return s.concat(s);
    }
}


class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // normalize and split the line,,\W+
        String[] tokens = value.toLowerCase().split("\\W+");

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}
