package cn.ljs.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Window TopN 不会出现回撤数据，
 * 因为 Window TopN 实现是在窗口结束时输出最终结果，不会产生中间结果。
 * 而且注意，因为是窗口上面的操作，Window TopN 在窗口结束时，会自动把 State 给清除。
 */
public class WindowTopNDemo {
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

        String sql = "CREATE TABLE source_table (\n" +
                "    name BIGINT NOT NULL,\n" +
                "    search_cnt BIGINT NOT NULL,\n" +
                "    key BIGINT NOT NULL,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");" +
                "CREATE TABLE sink_table (\n" +
                "    key BIGINT,\n" +
                "    name BIGINT,\n" +
                "    search_cnt BIGINT,\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");" +
                "INSERT INTO sink_table\n" +
                "SELECT key, name, search_cnt, window_start, window_end\n" +
                "FROM (\n" +
                "   SELECT key, name, search_cnt, window_start, window_end, \n" +
                "     ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key\n" +
                "       ORDER BY search_cnt desc) AS rownum\n" +
                "   FROM (\n" +
                "      SELECT window_start, window_end, key, name, max(search_cnt) as search_cnt\n" +
                "      -- window tvf 写法\n" +
                "      FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '1' MINUTES))\n" +
                "      GROUP BY window_start, window_end, key, name\n" +
                "   )\n" +
                ")\n" +
                "WHERE rownum <= 100";
    }
}
