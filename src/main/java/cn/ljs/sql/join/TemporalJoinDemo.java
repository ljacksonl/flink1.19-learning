package cn.ljs.sql.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Temporal Join（快照 Join）
 * 明细表Join快照表,订单金额表关联汇率表
 * 事件时间的 Temporal Join 一定要给左右两张表都设置 Watermark
 * 事件时间的 Temporal Join 一定要把 Versioned Table 的主键包含在 Join on 的条件中
 */
public class TemporalJoinDemo {
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

        //PRIMARY KEY 定义方式
        String versionedTable = "-- 定义一个汇率 versioned 表\n" +
                "CREATE TABLE currency_rates (\n" +
                "    currency STRING,\n" +
                "    conversion_rate DECIMAL(32, 2),\n" +
                "    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,\n" +
                "    WATERMARK FOR update_time AS update_time,\n" +
                "    -- PRIMARY KEY 定义方式\n" +
                "    PRIMARY KEY(currency) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'value.format' = 'debezium-json',\n" +
                "   /* ... */\n" +
                ");";

        //Deduplicate 定义方式
        String deduplicateTable = "-- 定义一个 append-only 的数据源表\n" +
                "CREATE TABLE currency_rates (\n" +
                "    currency STRING,\n" +
                "    conversion_rate DECIMAL(32, 2),\n" +
                "    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,\n" +
                "    WATERMARK FOR update_time AS update_time\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'value.format' = 'debezium-json',\n" +
                "    /* ... */\n" +
                ");\n" +
                "\n" +
                "-- 将数据源表按照 Deduplicate 方式定义为 Versioned Table\n" +
                "CREATE VIEW versioned_rates AS\n" +
                "SELECT currency, conversion_rate, update_time   -- 1. 定义 `update_time` 为时间字段\n" +
                "  FROM (\n" +
                "      SELECT *,\n" +
                "      ROW_NUMBER() OVER (PARTITION BY currency  -- 2. 定义 `currency` 为主键\n" +
                "         ORDER BY update_time DESC              -- 3. ORDER BY 中必须是时间戳列\n" +
                "      ) AS rownum \n" +
                "      FROM currency_rates)\n" +
                "WHERE rownum = 1;";

        String ordersTable = "CREATE TABLE orders (\n" +
                "    order_id    STRING,\n" +
                "    price       DECIMAL(32,2),\n" +
                "    currency    STRING,\n" +
                "    order_time  TIMESTAMP(3),\n" +
                "    WATERMARK FOR order_time AS order_time\n" +
                ") WITH (/* ... */);";

        //SQL 语法为：FOR SYSTEM_TIME AS OF
        String insertSql = "SELECT \n" +
                "     order_id,\n" +
                "     price,\n" +
                "     currency,\n" +
                "     conversion_rate,\n" +
                "     order_time,\n" +
                "FROM orders\n" +
                "-- 3. Temporal Join 逻辑\n" +
                "LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time\n" +
                "ON orders.currency = currency_rates.currency;";
    }
}
