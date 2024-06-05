package cn.ljs.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;

public class WordCountSqlFromElements {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        TableEnvironment tEnv = TableEnvironment.create(settings);
//        tEnv.getConfig()
//                .getConfiguration()
//                .setString("execution.runtime-mode", "BATCH");
//        tEnv.getConfig()
//                .getConfiguration()
//                .setString("table.exec.buffer-size", "1000");


        String words = "hello flink hello lujiansheng";
        String[] split = words.split("\\W+");
        ArrayList<WC> list = new ArrayList<>();

        for (String word : split) {
            WC wc = new WC(word, 1);
            list.add(wc);
        }
        DataStreamSource<WC> input = env.fromData(list);
        Table table = tableEnv.fromDataStream(input);
        table.printSchema();
        tableEnv.createTemporaryView("WordCount",table);
        Table table2 = tableEnv.sqlQuery("select word, sum(frequency) as frequency from WordCount group by word");
        DataStream<WC> ds3 = tableEnv.toDataStream(table2, WC.class);
        ds3.printToErr();
        env.execute();
        table2.execute().print();

    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return  word + ", " + frequency;
        }
    }
}
