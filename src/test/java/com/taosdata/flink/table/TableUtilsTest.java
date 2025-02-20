package com.taosdata.flink.table;

import com.taosdata.flink.common.TDengineConfigParams;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class TableUtilsTest {
    @Test
    public void testTDengineTableSinkCopy() throws Exception {
        System.out.println("testTDengineTableSinkCopy start！");
        Properties connProps = new Properties();
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        TDengineTableSink sink = new TDengineTableSink(connProps, Arrays.asList("a", "b"), 100);
        TDengineTableSink sinkClone = (TDengineTableSink)sink.copy();
        Assert.assertEquals(sinkClone.getSinkParallelism(), sink.getSinkParallelism());
        String value = sinkClone.getProperties().getProperty(TDengineConfigParams.VALUE_DESERIALIZER);
        Assert.assertEquals(value, "RowData");
        System.out.println("testTDengineTableSinkCopy finish！");
    }
}
