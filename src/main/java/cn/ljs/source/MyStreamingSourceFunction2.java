package cn.ljs.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

public class MyStreamingSourceFunction2 implements SourceFunction<MyStreamingSourceFunction.Item> {
    private boolean isRunning = true;
    @Override
    public void run(SourceContext<MyStreamingSourceFunction.Item> ctx) throws Exception {
        while(isRunning){
            MyStreamingSourceFunction.Item item = generateItem();
            System.out.println();
            ctx.collect(item);

            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    //随机产生一条商品数据
    private MyStreamingSourceFunction.Item generateItem(){
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        MyStreamingSourceFunction.Item item = new MyStreamingSourceFunction.Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        item.setTimestamp(System.currentTimeMillis());
        return item;
    }
}
