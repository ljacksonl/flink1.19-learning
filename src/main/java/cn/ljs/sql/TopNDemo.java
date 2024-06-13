package cn.ljs.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Arrays;

/**
 * TopN 其实就是对应到离线数仓中的 row_number()，可以使用 row_number() 对某一个分组的数据进行排序
 * 输出数据是有回撤数据的
 * 如果到达 N 条之后，经过 TopN 计算，发现这条数据比原有的数据排序靠前，
 * 那么新的 TopN 排名就会有变化，就变化了的这部分数据之前下发的排名数据撤回（即回撤数据），然后下发新的排名数据
 */
public class TopNDemo {
    public static void main(String[] args) {
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

        String sql = "-- 字段名        备注\n" +
                "-- key         搜索关键词\n" +
                "-- name        搜索热度名称\n" +
                "-- search_cnt   热搜消费热度（比如 3000）\n" +
                "-- timestamp       消费词条时间戳\n" +
                "\n" +
                "CREATE TABLE source_table (\n" +
                "    name BIGINT NOT NULL,\n" +
                "    search_cnt BIGINT NOT NULL,\n" +
                "    key BIGINT NOT NULL,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");\n" +
                "\n" +
                "-- 数据汇 schema：\n" +
                "\n" +
                "-- key         搜索关键词\n" +
                "-- name        搜索热度名称\n" +
                "-- search_cnt   热搜消费热度（比如 3000）\n" +
                "-- timestamp       消费词条时间戳\n" +
                "\n" +
                "CREATE TABLE sink_table (\n" +
                "    key BIGINT,\n" +
                "    name BIGINT,\n" +
                "    search_cnt BIGINT,\n" +
                "    `timestamp` TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");\n" +
                "\n" +
                "-- DML 逻辑\n" +
                "INSERT INTO sink_table\n" +
                "SELECT key, name, search_cnt, row_time as `timestamp`\n" +
                "FROM (\n" +
                "   SELECT key, name, search_cnt, row_time, \n" +
                "     -- 根据热搜关键词 key 作为 partition key，然后按照 search_cnt 倒排取前 100 名\n" +
                "     ROW_NUMBER() OVER (PARTITION BY key\n" +
                "       ORDER BY search_cnt desc) AS rownum\n" +
                "   FROM source_table)\n" +
                "WHERE rownum <= 100";


    }
}
