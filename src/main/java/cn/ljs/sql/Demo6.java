package cn.ljs.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Demo6 {
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
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceTableSql = "CREATE TABLE source_table (\n" +
                "    order_id BIGINT,\n" +
                "    product BIGINT,\n" +
                "    amount BIGINT,\n" +
                "    order_time as cast(CURRENT_TIMESTAMP as TIMESTAMP(3)),\n" +
                "    WATERMARK FOR order_time AS order_time - INTERVAL '0.001' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.order_id.min' = '1',\n" +
                "  'fields.order_id.max' = '2',\n" +
                "  'fields.amount.min' = '1',\n" +
                "  'fields.amount.max' = '10',\n" +
                "  'fields.product.min' = '1',\n" +
                "  'fields.product.max' = '2'\n" +
                ");";

        String sinkTableSql = "CREATE TABLE sink_table (\n" +
                "    product BIGINT,\n" +
                "    order_time TIMESTAMP(3),\n" +
                "    amount BIGINT,\n" +
                "    one_hour_prod_amount_sum BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");";

        String insertSql = "INSERT INTO sink_table\n" +
                "SELECT product, order_time, amount,\n" +
                "  SUM(amount) OVER (\n" +
                "    PARTITION BY product\n" +
                "    ORDER BY order_time\n" +
                "    -- 标识统计范围是一个 product 的最近 1 小时的数据\n" +
                "    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW\n" +
                "  ) AS one_hour_prod_amount_sum\n" +
                "FROM source_table";

        tEnv.executeSql(sourceTableSql);
        tEnv.executeSql(sinkTableSql);
        tEnv.executeSql(insertSql);
    }
}
