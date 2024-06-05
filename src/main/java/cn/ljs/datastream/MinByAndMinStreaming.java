package cn.ljs.datastream;

import cn.ljs.source.MyStreamingSourceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class MinByAndMinStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource<MyStreamingSourceFunction.Item> items = env.fromCollection(data);
        //min/max 会根据用户指定的字段取最小值，并且把这个值保存在对应的位置，而对于其他的字段，并不能保证其数值正确。
        items.keyBy(0).max(2).printToErr();
        items.keyBy(0).maxBy(2).printToErr();

        //打印结果
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }
}
