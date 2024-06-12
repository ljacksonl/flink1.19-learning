package cn.ljs.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class Demo1 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
//                .inBatchMode()
                .build();

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableEnvironment tEnv = TableEnvironment.create(settings);
        //一个 Flink 任务运行样例
        // 下面就是 Table API 的案例，其语义等同于
        // select a, count(b) as cnt
        // from Orders
        // group by a
        Table result = tEnv.from("Orders").groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));

        TableResult tableResult = tEnv.executeSql("SELECT * FROM " + result);
        tableResult.print();

    }
}
