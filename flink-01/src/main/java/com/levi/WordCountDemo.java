package com.levi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author: Levi
 * @Date: 2024/7/10 12:50
 */

public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDs = env.readTextFile("D:\\myspace\\java\\flink\\flink-learn-master\\flink-01\\input\\hello.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum =
                lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] lines = line.split(" ");
                for (String word : lines) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(value -> value.f0).sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum2 =
                lineDs.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] lines = line.split(" ");
            for (String word : lines) {
                out.collect(Tuple2.of(word, 1));
            }
        }).keyBy(value -> value.f0).sum(1);
        /*KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMap.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);*/
        sum.print();
        env.execute();
    }
    public static void main0(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDs = env.readTextFile("D:\\myspace\\java\\flink\\flink-learn-master\\flink-01\\input\\hello.txt");
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] elements = element.split(" ");
                for (String word : elements) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneUG = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> result = wordAndOneUG.sum(1);
        result.print();
    }
}