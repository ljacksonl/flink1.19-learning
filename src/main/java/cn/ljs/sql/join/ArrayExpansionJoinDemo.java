package cn.ljs.sql.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 数组列转行
 * 将表中 ARRAY 类型字段（列）拍平，转为多行
 */
public class ArrayExpansionJoinDemo {
    //比如某些场景下，日志是合并、攒批上报的，就可以使用这种方式将一个 Array 转为多行。
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

        String sourceLogTable = "CREATE TABLE show_log_table (\n" +
                "    log_id BIGINT,\n" +
                "    show_params ARRAY<STRING>\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.log_id.min' = '1',\n" +
                "  'fields.log_id.max' = '10'\n" +
                ");";

        String sinkTable = "CREATE TABLE sink_table (\n" +
                "    log_id BIGINT,\n" +
                "    show_param STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");";

        String insertSql = "INSERT INTO sink_table\n" +
                "SELECT\n" +
                "    log_id,\n" +
                "    t.show_param as show_param\n" +
                "FROM show_log_table\n" +
                "-- array 炸开语法\n" +
                "CROSS JOIN UNNEST(show_params) AS t (show_param)";

        tEnv.executeSql(sourceLogTable);
        tEnv.executeSql(sinkTable);
        tEnv.executeSql(insertSql);
    }
}
