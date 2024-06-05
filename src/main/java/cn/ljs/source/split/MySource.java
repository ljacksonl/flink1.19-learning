package cn.ljs.source.split;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MySource implements Source<String, MySourceSplit, List<MySourceSplit>> {
    private final int numSplits;

    public MySource(int numSplits) {
        this.numSplits = numSplits;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<String, MySourceSplit> createReader(SourceReaderContext context) {
        // 在这里创建并返回你的 SourceReader 实例
        return new SourceReader<String, MySourceSplit>() {
            @Override
            public void start() {

            }

            @Override
            public InputStatus pollNext(ReaderOutput<String> readerOutput) throws Exception {
                return null;
            }

            @Override
            public List<MySourceSplit> snapshotState(long l) {
                return null;
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return null;
            }

            @Override
            public void addSplits(List<MySourceSplit> list) {

            }

            @Override
            public void notifyNoMoreSplits() {

            }

            @Override
            public void close() throws Exception {

            }
        };
    }

    @Override
    public SplitEnumerator<MySourceSplit, List<MySourceSplit>> createEnumerator(SplitEnumeratorContext<MySourceSplit> context) {
        // 在这里创建并返回你的 SplitEnumerator 实例
        List<MySourceSplit> unassignedSplits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            unassignedSplits.add(new MySourceSplit(i));
        }
        return new MySplitEnumerator(context, unassignedSplits);
    }

    @Override
    public SplitEnumerator<MySourceSplit, List<MySourceSplit>> restoreEnumerator(SplitEnumeratorContext<MySourceSplit> context, List<MySourceSplit> checkpointedState) {
        // 在这里创建并返回你的 SplitEnumerator 实例，从检查点状态中恢复
        return new MySplitEnumerator(context, checkpointedState);
    }

    @Override
    public SimpleVersionedSerializer<MySourceSplit> getSplitSerializer() {
        // 在这里返回你的 SourceSplit 序列化器实例
        return new MySourceSplit.MySourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<List<MySourceSplit>> getEnumeratorCheckpointSerializer() {
        // 在这里返回你的 SplitEnumerator 检查点序列化器实例
//        return new MySplitEnumerator();
        return null;
    }
}