package cn.ljs.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WordCountFromElements {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.19版本，弃用Dataset,弃用Time类
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> text = env.fromData("Flink Spark Storm",
                "Flink Flink Flink",
                "Spark Spark Spark",
                "Storm Storm Storm");
        text.map(new MapFunction<String, List<Tuple2<String,Integer>>>() {
            @Override
            public List<Tuple2<String, Integer>> map(String value) throws Exception {
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                for (String word : value.split("\\W+")) {
                    list.add(new Tuple2<>(word,1));
                }
                return list;
            }
        }).print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataset = text.flatMap(new lineSplitter())
                .keyBy(value -> value.f0)
                .sum(1);
        dataset.print();
        env.execute("Window WordCount");
    }

    private static class lineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0){
                    out.collect(new Tuple2<>(token,1));
                }
            }
        }
    }
}
