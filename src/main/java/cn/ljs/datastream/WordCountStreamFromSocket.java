package cn.ljs.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WordCountStreamFromSocket {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(3);
        env.setBufferTimeout(100);
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("127.0.0.1", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .sum(1);
        dataStream.print();
        env.execute("Window WordCount");
    }

    private static class Splitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
