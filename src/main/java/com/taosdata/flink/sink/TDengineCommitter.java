package com.taosdata.flink.sink;

import org.apache.flink.api.connector.sink2.Committer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

public class TDengineCommitter implements Committer<TDengineCommittable>, Closeable {
    @Override
    public void commit(Collection<CommitRequest<TDengineCommittable>> committables) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws IOException {

    }
}
