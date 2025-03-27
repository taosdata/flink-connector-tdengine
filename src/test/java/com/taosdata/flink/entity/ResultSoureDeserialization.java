package com.taosdata.flink.entity;

import com.taosdata.flink.source.entity.SplitResultRecord;
import com.taosdata.flink.source.serializable.TDengineRecordDeserialization;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class ResultSoureDeserialization implements TDengineRecordDeserialization<ResultBean> {
    /**
     * @param splitResultRecord A record of information containing an object list
     * @return Data format after data conversion
     * @throws SQLException
     */
    @Override
    public ResultBean convert(SplitResultRecord splitResultRecord) throws SQLException {
        ResultBean resultBean = new ResultBean();
        ResultSetMetaData metaData = splitResultRecord.getMetaData();
        List<Object> rowData = splitResultRecord.getSourceRecordList();
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            if (metaData.getColumnName(i).equals("ts")) {
                resultBean.setTs((Timestamp) rowData.get(i - 1));
            } else if (metaData.getColumnName(i).equals("current")) {
                resultBean.setCurrent((Float) rowData.get(i - 1));
            } else if (metaData.getColumnName(i).equals("voltage")) {
                resultBean.setVoltage((Integer) rowData.get(i - 1));
            } else if (metaData.getColumnName(i).equals("phase")) {
                resultBean.setPhase((Float) rowData.get(i - 1));
            } else if (metaData.getColumnName(i).equals("location")) {
                resultBean.setLocation(new String ((byte[]) rowData.get(i - 1)));
            } else if (metaData.getColumnName(i).equals("groupid")) {
                resultBean.setGroupid((Integer) rowData.get(i - 1));
            } else if (metaData.getColumnName(i).equals("tbname")) {
                resultBean.setTbname(new String ((byte[]) rowData.get(i - 1)));
            }
        }
        return resultBean;
    }

    /**
     * @return Data Type after data conversion
     */
    @Override
    public TypeInformation<ResultBean> getProducedType() {
        return (TypeInformation.of(ResultBean.class));
    }
}
