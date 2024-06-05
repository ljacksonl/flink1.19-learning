package cn.ljs.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WordCountStreamFromSocket1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999, "\n");
        text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(new WordWithCount(word,1L));
                }
            }
        }).keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount wc) throws Exception {
                        return wc.word;
                    }
                })
                        .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(5),Duration.ofSeconds(1)));

        env.execute("Window WordCount");
    }

    private static class WordWithCount{
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
