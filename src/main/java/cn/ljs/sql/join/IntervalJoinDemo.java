package cn.ljs.sql.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Regular Join 会产生回撤流，但是在实时数仓中一般写入的 sink 都是类似于 Kafka 这样的消息队列，然后后面接 clickhouse 等引擎，
 * 这些引擎又不具备处理回撤流的能力。所以我理解 Interval Join 就是用于消灭回撤流的
 */
public class IntervalJoinDemo {
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

        String sourceShowSql = "CREATE TABLE show_log_table (\n" +
                "    log_id BIGINT,\n" +
                "    show_params STRING,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.show_params.length' = '1',\n" +
                "  'fields.log_id.min' = '1',\n" +
                "  'fields.log_id.max' = '10'\n" +
                ");";

        String sourceClickSql = "CREATE TABLE click_log_table (\n" +
                "    log_id BIGINT,\n" +
                "    click_params STRING,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.click_params.length' = '1',\n" +
                "  'fields.log_id.min' = '1',\n" +
                "  'fields.log_id.max' = '10'\n" +
                ");";

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    s_id BIGINT,\n" +
                "    s_params STRING,\n" +
                "    c_id BIGINT,\n" +
                "    c_params STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");";

        //实时 Regular Join 可以不是 等值 join。
        String queryInnerJoinSql = "INSERT INTO sink_table\n" +
                "SELECT\n" +
                "    show_log_table.log_id as s_id,\n" +
                "    show_log_table.show_params as s_params,\n" +
                "    click_log_table.log_id as c_id,\n" +
                "    click_log_table.click_params as c_params\n" +
                "FROM show_log_table INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id\n" +
                "AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '4' HOUR AND click_log_table.row_time;";

        //如果右流之后数据到达之后，发现左流之前输出过没有 Join 到的数据，则会发起回撤流，先输出 -[L, null]，然后输出 +[L, R]
        String queryLeftJoinSql = "INSERT INTO sink_table\n" +
                "SELECT\n" +
                "    show_log.log_id as s_id,\n" +
                "    show_log.show_params as s_params,\n" +
                "    click_log.log_id as c_id,\n" +
                "    click_log.click_params as c_params\n" +
                "FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id\n" +
                "AND show_log.row_time BETWEEN click_log.row_time - INTERVAL '5' SECOND AND click_log.row_time + INTERVAL '5' SECOND;";

//        String queryFullJoinSql = "INSERT INTO sink_table\n" +
//                "SELECT\n" +
//                "    show_log.log_id as s_id,\n" +
//                "    show_log.show_params as s_params,\n" +
//                "    click_log.log_id as c_id,\n" +
//                "    click_log.click_params as c_params\n" +
//                "FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id\n" +
//                "AND show_log.row_time BETWEEN click_log.row_time - INTERVAL '5' SECOND AND click_log.row_time + INTERVAL '5' SECOND;";



        tEnv.executeSql(sourceShowSql);
        tEnv.executeSql(sourceClickSql);
        tEnv.executeSql(sinkSql);
        tEnv.executeSql(queryInnerJoinSql);
//        tEnv.executeSql(queryLeftJoinSql);
//        tEnv.executeSql(queryFullJoinSql);
    }
}
