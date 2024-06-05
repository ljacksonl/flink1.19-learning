package cn.ljs.source;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyStreamingSourceFunction implements SourceFunction<MyStreamingSourceFunction.Item>{
    private Boolean isRunning = true;

    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning){
            Item item = generateItem();
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
    private Item generateItem(){
        int i = new Random().nextInt(100);

        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        item.setTimestamp(System.currentTimeMillis());
        return item;
    }
    public static class Item
    {
        private Integer id;
        private String name;
        private Long timestamp;

        Item() {
        }

        public String getName() {
            return name;
        }

        void setName(String name) {
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        void setId(Integer id) {
            this.id = id;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id + '\'' +
                    "timestamp=" + timestamp +
                    '}';
        }
    }


}
