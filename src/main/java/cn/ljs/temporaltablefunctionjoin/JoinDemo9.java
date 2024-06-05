package cn.ljs.temporaltablefunctionjoin;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * EventTime的一个实例
 */
public class JoinDemo9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定是EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.19版本，弃用Dataset,弃用Time类
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //主流，用户流, 格式为：user_name、city_id、ts
        List<Tuple3<String, Integer, Long>> list1 = new ArrayList<>();
        list1.add(new Tuple3<>("user1", 1001, 1L));
        list1.add(new Tuple3<>("user1", 1001, 10L));
        list1.add(new Tuple3<>("user2", 1002, 2L));
        list1.add(new Tuple3<>("user2", 1002, 15L));
        DataStream<Tuple3<String, Integer, Long>> textStream = env.fromCollection(list1)
                .assignTimestampsAndWatermarks(
                        //指定水位线、时间戳
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                                return element.f2;
                            }
                        }
                );

        //定义城市流,格式为：city_id、city_name、ts
        List<Tuple3<Integer, String, Long>> list2 = new ArrayList<>();
        list2.add(new Tuple3<>(1001, "beijing", 1L));
        list2.add(new Tuple3<>(1001, "beijing2", 10L));
        list2.add(new Tuple3<>(1002, "shanghai", 1L));
        list2.add(new Tuple3<>(1002, "shanghai2", 5L));

        DataStream<Tuple3<Integer, String, Long>> cityStream = env.fromCollection(list2)
                .assignTimestampsAndWatermarks(
                        //指定水位线、时间戳
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple3<Integer, String, Long> element) {
                                return element.f2;
                            }
                        });

        //转变为Table
        Table userTable = tableEnv.fromDataStream(textStream);
        Table cityTable = tableEnv.fromDataStream(cityStream);

        tableEnv.createTemporaryView("userTable", userTable);
        tableEnv.createTemporaryView("cityTable", cityTable);

        Expression ps = $("ts"); // 时间戳列
        Expression city_id = $("city_id"); // 键列
        //定义一个TemporalTableFunction
        TemporalTableFunction dimCity = cityTable.createTemporalTableFunction(ps, city_id);
        //注册表函数
        tableEnv.registerFunction("dimCity", dimCity);

        //关联查询
        Table result = tableEnv
                .sqlQuery("select u.user_name,u.city_id,d.city_name,u.ts from userTable as u " +
                        ", Lateral table (dimCity(u.ts)) d " +
                        "where u.city_id=d.city_id");

        //打印输出
        DataStream resultDs = tableEnv.toAppendStream(result, Row.class);
        resultDs.print();
        env.execute("joinDemo");
    }
}