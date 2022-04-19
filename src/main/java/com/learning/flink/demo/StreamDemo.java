package com.learning.flink.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
//import org.apache.flink.streaming.api.scala.DataStream;

public class StreamDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<String, Object> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(x -> x);

        KeyedStream<String, Object> streamOfWords  = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        List<String> test = new ArrayList<String>();
        test.add("AAA");
        test.add("BBB");
        test.add("CCC");
        DataStream df  = env.fromCollection(test);
        df.print();
        env.execute();
    }
}
