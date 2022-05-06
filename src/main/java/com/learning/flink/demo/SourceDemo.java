package com.learning.flink.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceDemo {

    public DataStream getStream(StreamExecutionEnvironment env){

        DataStream file_DF = env.readTextFile("input_path/input_text.txt");
        return file_DF;
    }

}
