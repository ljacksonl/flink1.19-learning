package cn.ljs.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * SQL API 创建外部数据表
 */
public class Demo3 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // SQL API 执行 create table 创建表
        //使用 Create Table xxx DDL 定义一个 Kafka 数据源（输入）表（也可以是 Kafka 数据汇（输出）表）。
        tEnv.executeSql(
                "CREATE TABLE KafkaSourceTable (\n"
                        + "  `f0` STRING,\n"
                        + "  `f1` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'topic',\n"
                        + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
                        + "  'properties.group.id' = 'testGroup',\n"
                        + "  'format' = 'json'\n"
                        + ")"
        );

        Table t = tEnv.sqlQuery("SELECT * FROM KafkaSourceTable");

        // Table API 中的一个 Table 对象
        Table projTable = tEnv.from("X").select();

        // 将 projTable 创建为一个叫做 projectedTable 的 VIEW
        tEnv.createTemporaryView("projectedTable", projTable);

        //SQL API 创建 VIEW
        String sql = "CREATE TABLE source_table (\n"
                + "    user_id BIGINT,\n"
                + "    `name` STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'user_defined',\n"
                + "  'format' = 'json',\n"
                + "  'class.name' = 'flink.examples.sql._03.source_sink.table.user_defined.UserDefinedSource'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "CREATE VIEW query_view as\n" // 创建 VIEW
                + "SELECT\n"
                + "    *\n"
                + "FROM source_table\n"
                + ";\n"
                + "INSERT INTO sink_table\n"
                + "SELECT\n"
                + "    *\n"
                + "FROM query_view;";
        Arrays.stream(sql.split(";"))
                .forEach(tEnv::executeSql);
    }
}
