package cn.ljs.sql.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;
import java.util.Arrays;

/**
 * 自定义列转行
 * Table Function 本质上是个 UDTF 函数，和离线 Hive SQL 一样，我们可以自定义 UDTF 去决定列转行的逻辑
 * Inner Join Table Function：如果 UDTF 返回结果为空，则相当于 1 行转为 0 行，这行数据直接被丢弃
 * Left Join Table Function：如果 UDTF 返回结果为空，折行数据不会被丢弃，只会在结果中填充 null 值
 */
public class TableFunctionJoinDemo {
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

        String sql = "CREATE FUNCTION user_profile_table_func AS 'cn.ljs.sql.join"
                + ".TableFunctionJoinDemo$UserProfileTableFunction';\n"
                + "\n"
                + "CREATE TABLE source_table (\n"
                + "    user_id BIGINT NOT NULL,\n"
                + "    name STRING,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.name.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '10'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    age INT,\n"
                + "    row_time TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT user_id,\n"
                + "       name,\n"
                + "       age,\n"
                + "       row_time\n"
                + "FROM source_table,\n"
                // Table Function Join 语法对应 LATERAL TABLE
                + "LATERAL TABLE(user_profile_table_func(user_id)) t(age)";

        Arrays.stream(sql.split(";"))
                .forEach(tEnv::executeSql);
    }

    public static class UserProfileTableFunction extends TableFunction<Integer> {

        public void eval(long userId) {
            // 自定义输出逻辑
            if (userId <= 5) {
                // 一行转 1 行
                collect(1);
            } else {
                // 一行转 3 行
                collect(1);
                collect(2);
                collect(3);
            }
        }

    }
}
