package com.learning.flink.demo;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    }
}
