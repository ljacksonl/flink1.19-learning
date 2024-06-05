package cn.ljs.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MyStreamingSource implements Source {

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext splitEnumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator restoreEnumerator(SplitEnumeratorContext splitEnumeratorContext, Object o) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return null;
    }
}
