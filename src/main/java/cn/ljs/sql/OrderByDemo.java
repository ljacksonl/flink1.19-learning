package cn.ljs.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Arrays;

public class OrderByDemo {
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

        /**
         * Order By 子句中必须要有时间属性字段，并且时间属性必须为升序时间属性，
         * 即 WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND
         * 或者 WATERMARK FOR rowtime_column AS rowtime_column
         */
        String sql = "CREATE TABLE source_table_1 (\n" +
                "    user_id BIGINT NOT NULL,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '10'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE sink_table (\n" +
                "    user_id BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "\n" +
                "INSERT INTO sink_table\n" +
                "SELECT user_id\n" +
                "FROM source_table_1\n" +
                "Order By row_time, user_id desc";

        Arrays.stream(sql.split(";"))
                .forEach(tEnv::executeSql);
    }
}
