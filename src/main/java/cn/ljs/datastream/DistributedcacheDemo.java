package cn.ljs.datastream;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DistributedcacheDemo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("/Users/lujiansheng/WorkSpace/quickstart/distributedcache.txt", "distributedCache");
        //1：注册一个文件,可以使用hdfs上的文件 也可以是本地文件进行测试
        DataStreamSource<String> data = env.fromData("Linea", "Lineb", "Linec", "Lined");

        SingleOutputStreamOperator<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2：使用该缓存文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("distributedCache");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.err.println("分布式缓存为:" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                //在这里就可以使用dataList
                System.err.println("使用datalist：" + dataList + "-------" + value);
                //业务逻辑
                return dataList + "：" + value;
            }
        });
        result.printToErr();
    }
}
