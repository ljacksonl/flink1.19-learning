package cn.ljs.datastream;

import cn.ljs.source.MyStreamingSourceFunction;
import cn.ljs.source.MyStreamingSourceFunction2;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class StreamingSplitFlow {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString("rest.port","9091"); //指定 Flink Web UI 端口为9091
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMinutes(1));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        //在 Flink 1.12 中，默认的流时间特征已更改为 EventTime，因此无需调用该方法即可启用 事件时间支持
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<MyStreamingSourceFunction.Item> source = env.addSource(new MyStreamingSourceFunction2());
//        // 使用 WatermarkStrategy 为数据流分配时间戳和水位线
//        WatermarkStrategy<MyStreamingSourceFunction.Item> watermarkStrategy = WatermarkStrategy
//                .<MyStreamingSourceFunction.Item>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner((item, timestamp) -> item.getTimestamp() != null ? item.getTimestamp() : timestamp)
//                .withIdleness(Duration.ofMinutes(1));
//        source.assignTimestampsAndWatermarks(watermarkStrategy);
        //分流,偶数
        SingleOutputStreamOperator<MyStreamingSourceFunction.Item> evenSelect = source.filter(new FilterFunction<MyStreamingSourceFunction.Item>() {
            @Override
            public boolean filter(MyStreamingSourceFunction.Item item) throws Exception {

                return item.getId() % 2 == 0;
            }
        });
        //分流,奇数
        SingleOutputStreamOperator<MyStreamingSourceFunction.Item> oddSelect = source.filter(new FilterFunction<MyStreamingSourceFunction.Item>() {
            @Override
            public boolean filter(MyStreamingSourceFunction.Item item) throws Exception {

                return item.getId() % 2 != 0;
            }
        });

        // 使用 AssignerWithPeriodicWatermarks 为数据流分配时间戳和水位线
//        evenSelect.assignTimestampsAndWatermarks(new WatermarkStrategy<MyStreamingSourceFunction.Item>() {
//            @Override
//            public WatermarkGenerator<MyStreamingSourceFunction.Item> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                return new WatermarkGenerator<MyStreamingSourceFunction.Item>() {
//                    private long maxTimestamp;
//                    private long delay = 3000;
//                    @Override
//                    public void onEvent(MyStreamingSourceFunction.Item item, long l, WatermarkOutput output) {
//                        maxTimestamp = Math.max(maxTimestamp, item.getTimestamp());
//                    }
//
//                    @Override
//                    public void onPeriodicEmit(WatermarkOutput output) {
//                        output.emitWatermark(new Watermark(maxTimestamp - delay));
//                    }
//                };
//            }
//        });
        evenSelect.printToErr();

//        evenSelect.keyBy(MyStreamingSourceFunction.Item::getId)
//                        .window(Tumble.over(lit(10).minute()).on($("user_action_time")).as("userActionWindow"))
        oddSelect.print();
        //table报错
        Table table = tableEnv.fromDataStream(evenSelect);
        tableEnv.createTemporaryView("table11", table);

        env.execute("streaming sql job");
//        table.execute().print();
    }
}
