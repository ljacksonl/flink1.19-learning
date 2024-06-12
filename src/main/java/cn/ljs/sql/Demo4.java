package cn.ljs.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 计算每一种商品（sku_id 唯一标识）的售出个数、总销售额、平均销售额、最低价、最高价
 * 数据源为商品的销售流水（sku_id：商品，price：销售价格），
 * 然后写入到 Kafka 的指定 topic（sku_id：商品，count_result：售出个数、sum_result：总销售额、avg_result：平均销售额、min_result：最低价、max_result：最高价）当中
 */
public class Demo4 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 声明为流任务
                //.inBatchMode() // 声明为批任务
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 1. 创建一个数据源（输入）表，这里的数据源是 flink 自带的一个随机 mock 数据的数据源。
        String sourceSql = "CREATE TABLE source_table (\n"
                + "    sku_id STRING,\n"
                + "    price BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.sku_id.length' = '1',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '1000000'\n"
                + ")";

        // 2. 创建一个数据汇（输出）表，输出到 kafka 中
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    sku_id STRING,\n"
                + "    count_result BIGINT,\n"
                + "    sum_result BIGINT,\n"
                + "    avg_result DOUBLE,\n"
                + "    min_result BIGINT,\n"
                + "    max_result BIGINT,\n"
                + "    PRIMARY KEY (`sku_id`) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'upsert-kafka',\n"
                + "  'topic' = 'tuzisir',\n"
                + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
                + "  'key.format' = 'json',\n"
                + "  'value.format' = 'json'\n"
                + ")";

        // 3. 执行一段 group by 的聚合 SQL 查询
        String selectWhereSql = "insert into sink_table\n"
                + "select sku_id,\n"
                + "       count(*) as count_result,\n"
                + "       sum(price) as sum_result,\n"
                + "       avg(price) as avg_result,\n"
                + "       min(price) as min_result,\n"
                + "       max(price) as max_result\n"
                + "from source_table\n"
                + "group by sku_id";

        tEnv.executeSql(sourceSql);
        tEnv.executeSql(sinkSql);
        tEnv.executeSql(selectWhereSql);
    }
}
