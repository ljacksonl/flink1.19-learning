package cn.ljs.datastream;

import cn.ljs.source.MyStreamingSource;
import cn.ljs.source.MyStreamingSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class StreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //注意：并行度设置为1
        DataStreamSource<MyStreamingSourceFunction.Item> text = env.addSource(new MyStreamingSourceFunction()).setParallelism(1);
        //map算子
        SingleOutputStreamOperator<String> item = text.map(MyStreamingSourceFunction.Item::getName);
        //flatmap算子
        SingleOutputStreamOperator<Object> flatMapItems = text.flatMap(new FlatMapFunction<MyStreamingSourceFunction.Item, Object>() {
            @Override
            public void flatMap(MyStreamingSourceFunction.Item item, Collector<Object> collector) throws Exception {
                String name = item.getName();
                collector.collect(name);
            }
        });
        //fliter算子
        SingleOutputStreamOperator<MyStreamingSourceFunction.Item> filterItems = text.filter(new FilterFunction<MyStreamingSourceFunction.Item>() {
            @Override
            public boolean filter(MyStreamingSourceFunction.Item item) throws Exception {
                return item.getId() % 2 == 0;
            }
        });





        //addSource过时，使用Source接口
        //使用集合
        List<Integer> data = Arrays.asList(1, 22, 3);
        DataStreamSource<Integer> ds = env.fromData(data);
        //自带的数据生成器
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.STRING
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(),"dataGeneratorSource").print();

        ds.printToErr().setParallelism(1);
        //打印结果
        item.print().setParallelism(1);
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }
    static class MyMapFunction extends RichMapFunction<MyStreamingSourceFunction.Item,String> {

        @Override
        public String map(MyStreamingSourceFunction.Item item) throws Exception {
            return item.getName();
        }
    }
}
