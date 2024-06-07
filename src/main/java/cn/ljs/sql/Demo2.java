package cn.ljs.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Demo2 {
    public static void main(String[] args) {
        //一个 SQL\Table API 任务的代码结构
        // 创建一个 TableEnvironment，为后续使用 SQL 或者 Table API 提供上线
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 声明为流任务
                //.inBatchMode() // 声明为批任务
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        /**
         *     // 创建一个输入表
         *         tEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )");
         *     // 创建一个输出表
         *         tEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");
         *
         *     // 1. 使用 Table API 做一个查询并返回 Table
         *         Table table2 = tEnv.from("table1").select(...);
         *     // 2. 使用 SQl API 做一个查询并返回 Table
         *         Table table3 = tEnv.sqlQuery("SELECT ... FROM table1 ... ");
         *
         *     // 将 table2 的结果使用 Table API 写入 outputTable 中，并返回结果
         *         TableResult tableResult = table2.executeInsert("outputTable");
         *         tableResult...
         */

    }
}
