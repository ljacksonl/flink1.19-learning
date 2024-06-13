package cn.ljs.sql.join;

/**
 * 维表 Join(实时获取外部缓存的 Join)
 *  Lookup Join 是流与 Redis，Mysql，HBase 这种存储介质的 Join。
 */
public class LookupJoinDemo {
    //使用曝光用户日志流（show_log）关联用户画像维表（user_profile）
    public static void main(String[] args) {
        String sourceLogTable = "CREATE TABLE show_log (\n" +
                "    log_id BIGINT,\n" +
                "    `timestamp` as cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    user_id STRING,\n" +
                "    proctime AS PROCTIME()\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.user_id.length' = '1',\n" +
                "  'fields.log_id.min' = '1',\n" +
                "  'fields.log_id.max' = '10'\n" +
                ");";

        String sourceLookupTable = "CREATE TABLE user_profile (\n" +
                "    user_id STRING,\n" +
                "    age STRING,\n" +
                "    sex STRING\n" +
                "    ) WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'hostname' = '127.0.0.1',\n" +
                "  'port' = '6379',\n" +
                "  'format' = 'json',\n" +
                "  'lookup.cache.max-rows' = '500',\n" +
                "  'lookup.cache.ttl' = '3600',\n" +
                "  'lookup.max-retries' = '1'\n" +
                ");";

        String sinkTable = "CREATE TABLE sink_table (\n" +
                "    log_id BIGINT,\n" +
                "    `timestamp` TIMESTAMP(3),\n" +
                "    user_id STRING,\n" +
                "    proctime TIMESTAMP(3),\n" +
                "    age STRING,\n" +
                "    sex STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");";

        String lookUpJoinQuery = "INSERT INTO sink_table\n" +
                "SELECT \n" +
                "    s.log_id as log_id\n" +
                "    , s.`timestamp` as `timestamp`\n" +
                "    , s.user_id as user_id\n" +
                "    , s.proctime as proctime\n" +
                "    , u.sex as sex\n" +
                "    , u.age as age\n" +
                "FROM show_log AS s\n" +
                "LEFT JOIN user_profile FOR SYSTEM_TIME AS OF s.proctime AS u\n" +
                "ON s.user_id = u.user_id";
    }
}
