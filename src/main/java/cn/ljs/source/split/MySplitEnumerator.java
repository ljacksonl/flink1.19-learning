package cn.ljs.source.split;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MySplitEnumerator implements SplitEnumerator<MySourceSplit, List<MySourceSplit>> {
    private final SplitEnumeratorContext<MySourceSplit> context;
    private final List<MySourceSplit> unassignedSplits;

    public MySplitEnumerator(SplitEnumeratorContext<MySourceSplit> context, List<MySourceSplit> unassignedSplits) {
        this.context = context;
        this.unassignedSplits = unassignedSplits;
    }

    @Override
    public void start() {
        // 在这里可以添加初始化逻辑，例如从外部系统获取分片信息
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // 在这里可以添加处理分片请求的逻辑，例如将未分配的分片分配给请求的子任务
        if (!unassignedSplits.isEmpty()) {
            MySourceSplit split = unassignedSplits.remove(0);
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<MySourceSplit> splits, int subtaskId) {
        // 在这里可以添加将分片添加回未分配分片列表的逻辑
        unassignedSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // 在这里可以添加添加新的读取器的逻辑，例如将未分配的分片分配给新的子任务
    }

    @Override
    public List<MySourceSplit> snapshotState(long l) throws Exception {
        // 在这里可以添加创建状态快照的逻辑
        return new ArrayList<>(unassignedSplits);
    }


    @Override
    public void close() throws IOException {
        // 在这里可以添加关闭资源的逻辑
    }
}