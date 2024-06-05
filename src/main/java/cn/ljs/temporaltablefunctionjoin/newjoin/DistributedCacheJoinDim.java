package cn.ljs.temporaltablefunctionjoin.newjoin;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.HashMap;
import java.util.List;

/**
 * 通过Distributed Cache实现维度关联
 */
@Slf4j
public class DistributedCacheJoinDim {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册缓存文件 如: file:///some/path 或 hdfs://host:port/and/path
        String cachedFilePath = "./user_info.txt";
        String cachedFileName = "user_info";
        env.registerCachedFile(cachedFilePath, cachedFileName);

        // 添加实时流
        DataStreamSource<Tuple2<String, String>> stream = env.fromElements(
                Tuple2.of("1", "click"),
                Tuple2.of("2", "click"),
                Tuple2.of("3", "browse"));

        // 关联维度
        SingleOutputStreamOperator<String> dimedStream = stream.flatMap(new RichFlatMapFunction<Tuple2<String, String>, String>() {

            HashMap dimInfo = new HashMap<String, Integer>();

            // 读取文件
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File cachedFile = getRuntimeContext().getDistributedCache().getFile(cachedFileName);
                List<String> lines = FileUtils.readLines(cachedFile);
                for (String line : lines) {
                    String[] split = line.split(",");
                    dimInfo.put(split[0], Integer.valueOf(split[1]));
                }
            }

            // 关联维度
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                if (dimInfo.containsKey(value.f0)) {
                    Integer age = (Integer) dimInfo.get(value.f0);
                    out.collect(value.f0 + "," + value.f1 + "," + age);
                }
            }
        });

        dimedStream.print();

        env.execute();
    }
}