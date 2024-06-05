package cn.ljs.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MySourceSplit implements SourceSplit {
    private final int splitId;

    public MySourceSplit(int splitId) {
        this.splitId = splitId;
    }

    public int getSplitId() {
        return splitId;
    }

    @Override
    public String splitId() {
        return String.valueOf(splitId);
    }

    public static class MySourceSplitSerializer implements SimpleVersionedSerializer<MySourceSplit> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(MySourceSplit split) throws IOException {
            return ByteBuffer.allocate(4).putInt(split.getSplitId()).array();
        }

        @Override
        public MySourceSplit deserialize(int version, byte[] serialized) throws IOException {
            return new MySourceSplit(ByteBuffer.wrap(serialized).getInt());
        }
    }
}