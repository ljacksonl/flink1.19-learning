package cn.ljs.sql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 在 pdd 这种发补贴券的场景下，希望可以在发的补贴券总金额超过 1w 元时，及时报警出来，来帮助控制预算，防止发的太多。
 * 这时候就需要SQL 与 DataStream API 的转换
 */
@Slf4j
public class Demo5 {

    public static void main(String[] args) throws Exception {

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




        // 1. pdd 发补贴券流水数据
        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n" // 补贴券的流水 id
                + "    money BIGINT,\n" // 补贴券的金额
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '10000',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";

        // 2. 计算总计发放补贴券的金额
        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n" // 补贴券的发放总金额
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        tEnv.executeSql(createTableSql);

        Table resultTable = tEnv.sqlQuery(querySql);

        // 3. 将金额结果转为 DataStream，然后自定义超过 1w 的报警逻辑
        tEnv.toDataStream(resultTable, Row.class)
                .flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public void flatMap(Row value, Collector<Object> out) throws Exception {
                        long l = Long.parseLong(String.valueOf(value.getField("sum_money")));

                        if (l > 10000L) {
                            log.info("报警，超过 1w");
                            System.out.println("报警，超过 1w");
                        }
                    }
                }).printToErr();

        env.execute();
    }
}
