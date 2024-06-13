package cn.ljs.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Arrays;

/**
 * 与TopN 中 row_number = 1 的场景一致，但是这里有一点不一样在于其排序字段一定是时间属性列，不能是其他非时间属性的普通列。
 * 在 row_number = 1 时，如果排序字段是普通列 planner 会翻译成 TopN 算子，
 * 如果是时间属性列 planner 会翻译成 Deduplication，这两者最终的执行算子是不一样的，Deduplication 相比 TopN 算子专门做了对应的优化，性能会有很大提升。
 */
public class DeduplicationDemo {
    public static void main(String[] args) {
        //腾讯 QQ 用户等级的场景，每一个 QQ 用户都有一个 QQ 用户等级，需要求出当前用户等级在 星星，月亮，太阳 的用户数分别有多少。
        /**
         * 在 Deduplication 关于是否会出现回撤流
         * Order by 事件时间 DESC：会出现回撤流，因为当前 key 下 可能会有 比当前事件时间还大的数据
         * Order by 事件时间 ASC：会出现回撤流，因为当前 key 下 可能会有 比当前事件时间还小的数据
         * Order by 处理时间 DESC：会出现回撤流，因为当前 key 下 可能会有 比当前处理时间还大的数据
         * Order by 处理时间 ASC：不会出现回撤流，因为当前 key 下 不可能会有 比当前处理时间还小的数据
         */
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 声明为流任务
                //.inBatchMode() // 声明为批任务
                .build();
        Configuration config = new Configuration();
        config.setString("rest.port","9091"); //指定 Flink Web UI 端口为9091
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMinutes(1));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);

        String sql = "CREATE TABLE source_table (\n" +
                "    user_id BIGINT COMMENT '用户 id',\n" +
                "    level STRING COMMENT '用户等级',\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)) COMMENT '事件时间戳',\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.level.length' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '1000000'\n" +
                ");\n" +
                "\n" +
                "-- 数据汇：输出即每一个等级的用户数\n" +
                "CREATE TABLE sink_table (\n" +
                "    level STRING COMMENT '等级',\n" +
                "    uv BIGINT COMMENT '当前等级用户数',\n" +
                "    row_time timestamp(3) COMMENT '时间戳'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "\n" +
                "-- 处理逻辑：\n" +
                "INSERT INTO sink_table\n" +
                "select \n" +
                "    level\n" +
                "    , count(1) as uv\n" +
                "    , max(row_time) as row_time\n" +
                "from (\n" +
                "      SELECT\n" +
                "          user_id,\n" +
                "          level,\n" +
                "          row_time,\n" +
                "          row_number() over(partition by user_id order by row_time) as rn\n" +
                "      FROM source_table\n" +
                ")\n" +
                "where rn = 1\n" +
                "group by \n" +
                "    level";

        Arrays.stream(sql.split(";"))
                .forEach(tEnv::executeSql);
    }
}
