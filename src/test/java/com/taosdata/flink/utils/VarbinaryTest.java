package com.taosdata.flink.utils;


import com.taosdata.flink.cdc.enumerator.TDengineCdcEnumState;
import com.taosdata.flink.cdc.serializable.TDengineCdcEnumStateSerializer;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.common.VersionComparator;
import com.taosdata.flink.sink.entity.SinkError;
import com.taosdata.flink.sink.entity.SinkErrorNumbers;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.serializer.RowDataSerializerBase;
import com.taosdata.flink.source.entity.*;
import com.taosdata.flink.source.enumerator.TDengineSourceEnumState;
import com.taosdata.flink.source.enumerator.TDengineSourceEnumerator;
import com.taosdata.flink.source.serializable.TDengineSourceEnumStateSerializer;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.jdbc.TSDBErrorNumbers;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.Test;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.taosdata.flink.common.Utils.trimBackticks;
import static com.taosdata.flink.sink.entity.DataType.DATA_TYPE_INT;
import static org.junit.jupiter.api.Assertions.*;

public class VarbinaryTest {
    @Test
    public void testVersionComparator() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                VersionComparator.compareVersion(null, "1.1.1.8"));

        assertThrows(IllegalArgumentException.class, () ->
                VersionComparator.compareVersion("1.1.1.8", null));

        assertThrows(IllegalArgumentException.class, () ->
                VersionComparator.compareVersion("a.b.c.d", "1.1.1.8"));

        assertEquals(0, VersionComparator.compareVersion("1.0.0", "1.0.0"));
        assertEquals(1, VersionComparator.compareVersion("1.1.0", "1.0.0"));
        assertEquals(0, VersionComparator.compareVersion("1.1.0.sssss.aaa", "1.1.0"));
        assertEquals(-1, VersionComparator.compareVersion("1.0.0", "1.1.0"));

    }

    @Test
    public void testCdcEnumStateSerializer() throws Exception {
        TDengineCdcSplit cdcSplit1 = new TDengineCdcSplit("topic1", "group1", "client1");
        Deque<TDengineCdcSplit> unassignedCdcSplits = new ArrayDeque<>(1);
        unassignedCdcSplits.add(cdcSplit1);

        List<TDengineCdcSplit> assignmentCdcSplits = new ArrayList<>(1);
        TDengineCdcSplit cdcSplit2 = new TDengineCdcSplit("topic0", "group0", "client0");
        assignmentCdcSplits.add(cdcSplit2);

        TDengineCdcEnumState enumState = new TDengineCdcEnumState(unassignedCdcSplits, assignmentCdcSplits, true);
        TDengineCdcEnumStateSerializer stateSerializer = new TDengineCdcEnumStateSerializer();

        byte[] data = stateSerializer.serialize(enumState);
        TDengineCdcEnumState state = stateSerializer.deserialize(stateSerializer.getVersion(), data);
        Deque<TDengineCdcSplit> cdcSplits = state.getUnassignedCdcSplits();
        TDengineCdcSplit split = cdcSplits.pop();
        assertTrue(split.equals(cdcSplit1));

        List<TDengineCdcSplit> splits = state.getAssignmentCdcSplits();
        assertTrue(splits.get(0).equals(cdcSplit2));
    }

    @Test
    public void testSourceEnumStateSerializer() throws Exception {
        String splitId = "1";
        List<String> taskList = Stream.of("1").collect(Collectors.toList());
        List<String> finishList = Stream.of("1").collect(Collectors.toList());

        TDengineSplit split1 = new TDengineSplit(splitId, taskList, finishList);
        Deque<TDengineSplit> unassignedSplits = new ArrayDeque<>(1);
        unassignedSplits.add(split1);

        TreeSet<TDengineSplit> assignmentSqls = new TreeSet<>();
        String splitId2 = "2";
        List<String> taskList2 = Stream.of("2").collect(Collectors.toList());
        List<String> finishList2 = Stream.of("2").collect(Collectors.toList());
        TDengineSplit split2 = new TDengineSplit(splitId2, taskList2, finishList2);
        assignmentSqls.add(split2);
        TDengineSourceEnumState enumState = new TDengineSourceEnumState(unassignedSplits, assignmentSqls, true);

        TDengineSourceEnumStateSerializer stateSerializer = new TDengineSourceEnumStateSerializer();

        byte[] data = stateSerializer.serialize(enumState);
        TDengineSourceEnumState state = stateSerializer.deserialize(stateSerializer.getVersion(), data);

        Deque<TDengineSplit> splits = state.getUnassignedSqls();
        TDengineSplit split = splits.pop();
        assertTrue(split.equals(split1));

        TreeSet<TDengineSplit> splitTreeSet = state.getAssignmentSqls();
        assertTrue(splitTreeSet.first().equals(split2));
    }

    @Test
    public void testSinkError() {
        SQLException sqlException = SinkError.createSQLException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
        assertEquals(SinkErrorNumbers.ERROR_DB_NAME_NULL, sqlException.getErrorCode());

        sqlException = SinkError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
        assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, sqlException.getErrorCode());

        sqlException = SinkError.createSQLException(TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED);
        assertEquals(0, sqlException.getErrorCode());

        sqlException = SinkError.createSQLException(0x2340, "test---test");
        assertEquals(0x2340, sqlException.getErrorCode());

        sqlException = SinkError.createSQLException(0x2360, "test---test");
        assertEquals(0x2360, sqlException.getErrorCode());

        sqlException = SinkError.createSQLException(0x2380, "test---test");
        assertEquals(0x2380, sqlException.getErrorCode());

        SinkError.createRuntimeException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
        SinkError.createRuntimeException(SinkErrorNumbers.ERROR_DB_NAME_NULL, "ERROR_DB_NAME_NULL");
        SinkError.createIllegalArgumentException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
        SinkError.createIllegalStateException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
        SinkError.createTimeoutException(SinkErrorNumbers.ERROR_DB_NAME_NULL, "timeout");
        SinkError.createSQLWarning("test");


        sqlException = SourceError.createSQLException(SourceErrorNumbers.ERROR_SERVER_ADDRESS);
        assertEquals(SourceErrorNumbers.ERROR_SERVER_ADDRESS, sqlException.getErrorCode());
        sqlException = SourceError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
        assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, sqlException.getErrorCode());

        sqlException = SourceError.createSQLException(TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED);
        assertEquals(0, sqlException.getErrorCode());

        sqlException = SourceError.createSQLException(0x2340, "test---test");
        assertEquals(0x2340, sqlException.getErrorCode());

        sqlException = SourceError.createSQLException(0x2360, "test---test");
        assertEquals(0x2360, sqlException.getErrorCode());

        sqlException = SourceError.createSQLException(0x2380, "test---test");
        assertEquals(0x2380, sqlException.getErrorCode());

        SourceError.createRuntimeException(SourceErrorNumbers.ERROR_SERVER_ADDRESS);
        SourceError.createRuntimeException(SourceErrorNumbers.ERROR_SERVER_ADDRESS, "ERROR_SERVER_ADDRESS");
        SourceError.createIllegalArgumentException(SourceErrorNumbers.ERROR_SERVER_ADDRESS);
        SourceError.createIllegalStateException(SourceErrorNumbers.ERROR_SERVER_ADDRESS);
        SourceError.createTimeoutException(SourceErrorNumbers.ERROR_SERVER_ADDRESS, "timeout");
        SourceError.createSQLWarning("test");
    }

    @Test
    public void testTimestampSplitInfo() {
        LocalDateTime startDateTime = LocalDateTime.now();
        LocalDateTime endDateTime = startDateTime.plusHours(1);
        long startTime = startDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long endTime = endDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        TimestampSplitInfo timestampSplitInfo = new TimestampSplitInfo(startDateTime, endDateTime, "ts", Duration.ofMinutes(10), ZoneId.systemDefault());
        assertEquals(startTime, timestampSplitInfo.getStartTime());
        assertEquals(endTime, timestampSplitInfo.getEndTime());

        timestampSplitInfo = new TimestampSplitInfo(startDateTime, endDateTime, "ts", Duration.ofMinutes(10), null);
        assertEquals(startTime, timestampSplitInfo.getStartTime());
        assertEquals(endTime, timestampSplitInfo.getEndTime());

        timestampSplitInfo = new TimestampSplitInfo(startTime, endTime, "ts", Duration.ofMinutes(10));
        assertEquals(startTime, timestampSplitInfo.getStartTime());
        assertEquals(endTime, timestampSplitInfo.getEndTime());
    }

    @Test
    public void testTDengineSourceEnumerator() throws Exception {
        SplitEnumeratorContext<TDengineSplit> context = new TestingSplitEnumeratorContext<>(1);
        SourceSplitSql sql = new SourceSplitSql();
        sql.setSelect("ts, `current`, voltage, phase, location, groupid, tbname");
        sql.setTableName("meters");
        sql.setWhere("voltage > 200");
        sql.setOther("order by ts desc");
        sql.setSplitType(SplitType.SPLIT_TYPE_SQL);
        TDengineSourceEnumState splitsState = new TDengineSourceEnumState(new ArrayDeque<>(1), new TreeSet<>(), true);
        TDengineSourceEnumerator enumerator = new TDengineSourceEnumerator(context, Boundedness.BOUNDED, sql, splitsState);
        enumerator.start();

        splitsState = new TDengineSourceEnumState(new ArrayDeque<>(1), new TreeSet<>(), false);
        enumerator = new TDengineSourceEnumerator(context, Boundedness.BOUNDED, sql, splitsState);
        enumerator.start();
        TDengineSourceEnumState state = enumerator.snapshotState(1);
        assertEquals(state.getUnassignedSqls().getFirst().getTaskSplits().get(0),
                "select ts, `current`, voltage, phase, location, groupid, tbname from `meters` where voltage > 200 order by ts desc");

        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select ts, `current`, voltage, phase, groupid, location from meters")
                .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
                //按照时间分片
                .setTimestampSplitInfo(new TimestampSplitInfo(
                        "2024-12-19 16:12:48.000",
                        "2024-12-19 19:12:48.000",
                        "ts",
                        Duration.ofDays(1),
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                        ZoneId.of("Asia/Shanghai")));
        enumerator = new TDengineSourceEnumerator(context, Boundedness.BOUNDED, splitSql, splitsState);
        enumerator.start();
        state = enumerator.snapshotState(1);
        assertNotNull(state.getUnassignedSqls().getFirst().getTaskSplits().get(0));

        splitSql = new SourceSplitSql();
        splitSql.setSql("select ts, `current`, voltage, phase, groupid, location from meters")
                .setSplitType(SplitType.SPLIT_TYPE_TAG)
                //按照时间分片
                .setTagList(Arrays.asList(
                        "groupid >100 and location = 'SuhctA'",
                        "groupid >50 and groupid < 100 and location = 'SuhctB'",
                        "groupid >0 and groupid < 50 and location = 'SuhctC'"));
        enumerator = new TDengineSourceEnumerator(context, Boundedness.BOUNDED, splitSql, splitsState);
        enumerator.start();
        state = enumerator.snapshotState(1);
        assertNotNull(state.getUnassignedSqls().getFirst().getTaskSplits().get(0));

    }

    @Test
    public void testTrimBackticks() throws Exception {
        assertNull(trimBackticks(null));
        assertEquals("SSAA", trimBackticks("SSAA"));
        assertEquals("", trimBackticks(""));
        assertEquals("", trimBackticks("``"));
        assertEquals("`", trimBackticks("```"));
        try {
            trimBackticks("`");
            trimBackticks("`sss");
            assertEquals("", "Field name anti quotation mark mismatch, no exception thrown");
        }catch (SQLException e) {
            System.out.println("testTrimBackticks SQLException ok! " + e.getMessage());
        }
    }

    @Test
    public void testFieldCountMismatch() {
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 10);
        rowData.setField(1, 20);

        List<SinkMetaInfo> metaInfos = Collections.singletonList(
                new SinkMetaInfo(false, DATA_TYPE_INT, "col1", 4)
        );
        RowDataSerializerBase base = new RowDataSerializerBase();
        Exception exception = assertThrows(SQLException.class, () ->
                base.getSinkRecord(rowData, metaInfos)
        );

        assertTrue(exception.getMessage().contains("The recod fields size (2) is not equal to the meta fields size(1)"));
    }

    @Test
    public void testTypeConversionError() {
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, StringData.fromString("not_a_int"));

        RowDataSerializerBase base = new RowDataSerializerBase();
        List<SinkMetaInfo> metaInfos = Collections.singletonList(
                new SinkMetaInfo(false, DATA_TYPE_INT, "col1", 4)
        );

        Exception exception = assertThrows(SQLException.class, () ->
                base.getSinkRecord(rowData, metaInfos)
        );

        assertTrue(exception.getMessage().contains("Error processing field index 0"));
        assertTrue(exception.getMessage().contains("Type: DATA_TYPE_INT"));
        assertTrue(exception.getMessage().contains("Name: col1"));
    }
}
