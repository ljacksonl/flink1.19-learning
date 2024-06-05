package cn.ljs.temporaltablefunctionjoin;

import com.google.common.cache.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 热存储维表
 * 使用cache来减轻访问压力
 */
public class JoinDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：user,1000,分别是用户名称和城市编号
                    String[] list = p.split(",");
                    return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        DataStream<Tuple3<String, Integer, String>> result = textStream.map(new MapJoinDemo1());
        result.print();
        env.execute("joinDemo1");
    }

    static class MapJoinDemo1 extends RichMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>> {
        LoadingCache<Integer, String> dim;

        @Override
        public void open(Configuration parameters) throws Exception {
            //使用google LoadingCache来进行缓存
            dim = CacheBuilder.newBuilder()
                    //最多缓存个数，超过了就根据最近最少使用算法来移除缓存
                    .maximumSize(1000)
                    //在更新后的指定时间后就回收
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    //指定移除通知
                    .removalListener(new RemovalListener<Integer, String>() {
                        @Override
                        public void onRemoval(RemovalNotification<Integer, String> removalNotification) {
                            System.out.println(removalNotification.getKey() + "被移除了，值为：" + removalNotification.getValue());
                        }
                    })
                    .build(
                            //指定加载缓存的逻辑
                            new CacheLoader<Integer, String>() {
                                @Override
                                public String load(Integer cityId) throws Exception {
                                    String cityName = readFromHbase(cityId);
                                    return cityName;
                                }
                            }
                    );

        }

        private String readFromHbase(Integer cityId) {
            //读取hbase
            //这里写死，模拟从hbase读取数据
            Map<Integer, String> temp = new HashMap<>();
            temp.put(1001, "beijing");
            temp.put(1002, "shanghai");
            temp.put(1003, "wuhan");
            temp.put(1004, "changsha");
            String cityName = "";
            if (temp.containsKey(cityId)) {
                cityName = temp.get(cityId);
            }

            return cityName;
        }

        @Override
        public Tuple3<String, Integer, String> map(Tuple2<String, Integer> value) throws Exception {
            //在map方法中进行主流和维表的关联
            String cityName = "";
            if (dim.get(value.f1) != null) {
                cityName = dim.get(value.f1);
            }
            return new Tuple3<>(value.f0, value.f1, cityName);
        }
    }
}