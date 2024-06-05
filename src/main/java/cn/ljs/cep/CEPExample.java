package cn.ljs.cep;

import cn.ljs.cep.model.Alert;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class CEPExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个数据流
        DataStream<Tuple3<String, String, Long>> source = env.fromElements(
                Tuple3.of("Marry", "外套", 1L),
                Tuple3.of("Marry", "帽子", 1L),
                Tuple3.of("Marry", "帽子", 2L),
                Tuple3.of("Marry", "帽子", 3L),
                Tuple3.of("Ming", "衣服", 1L),
                Tuple3.of("Marry", "鞋子", 1L),
                Tuple3.of("Marry", "鞋子", 2L),
                Tuple3.of("LiLei", "帽子", 1L),
                Tuple3.of("LiLei", "帽子", 2L),
                Tuple3.of("LiLei", "帽子", 3L)
        );

        // 定义事件模式序列
        Pattern<Tuple3<String, String, Long>, ?> pattern = Pattern.<Tuple3<String, String, Long>>begin("first").where(new SimpleCondition<Tuple3<String, String, Long>>() {
            @Override
            public boolean filter(Tuple3<String, String, Long> event) {
                return event.f1.equals("帽子");
            }
        }).next("second").where(new SimpleCondition<Tuple3<String, String, Long>>() {
            @Override
            public boolean filter(Tuple3<String, String, Long> event) {
                return event.f1.equals("帽子");
            }
        });

        // 应用模式序列
        DataStream<Alert> result = CEP.pattern(source, pattern).select(new PatternSelectFunction<Tuple3<String, String, Long>, Alert>() {
            @Override
            public Alert select(Map<String, List<Tuple3<String, String, Long>>> pattern) {
                List<Tuple3<String, String, Long>> firstEvents = pattern.get("first");
                List<Tuple3<String, String, Long>> secondEvents = pattern.get("second");

                // 检查两个连续的帽子事件是否属于同一个用户
                if (firstEvents.get(0).f0.equals(secondEvents.get(0).f0)) {
                    return new Alert("User " + firstEvents.get(0).f0 + " searched for the hat twice consecutively.");
                } else {
                    return null;
                }
            }
        });

        // 输出结果
        result.print();

        env.execute("CEP Example");
    }
}