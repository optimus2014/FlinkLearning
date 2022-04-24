package com.learning.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.*;    // 这个包引入是必须的
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ReadFileDemo {

    static StreamExecutionEnvironment env;
    public static void main(String[] args) throws Exception{
        env = StreamExecutionEnvironment.createLocalEnvironment();

        readFromObject();
//        readFromFile();
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
        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                file_DF.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .sum(1);
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
        // normalize and split the line
        String[] tokens = value.toLowerCase().split("\\W+");

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}
