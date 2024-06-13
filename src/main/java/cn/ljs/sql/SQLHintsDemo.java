package cn.ljs.sql;

public class SQLHintsDemo {
    public static void main(String[] args) {
        //启动一个 SQL CLI 之后，在 SQL CLI 中可以进行以下 SET 设置：
        String sql = "CREATE TABLE kafka_table1 (id BIGINT, name STRING, age INT) WITH (...);\n" +
                "CREATE TABLE kafka_table2 (id BIGINT, name STRING, age INT) WITH (...);\n" +
                "\n" +
                "-- 1. 使用 'scan.startup.mode'='earliest-offset' 覆盖原来的 scan.startup.mode\n" +
                "select id, name from kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;\n" +
                "\n" +
                "-- 2. 使用 'scan.startup.mode'='earliest-offset' 覆盖原来的 scan.startup.mode\n" +
                "select * from\n" +
                "    kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1\n" +
                "    join\n" +
                "    kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2\n" +
                "    on t1.id = t2.id;\n" +
                "\n" +
                "-- 3. 使用 'sink.partitioner'='round-robin' 覆盖原来的 Sink 表的 sink.partitioner\n" +
                "insert into kafka_table1 /*+ OPTIONS('sink.partitioner'='round-robin') */ select * from kafka_table2;";
    }
}
