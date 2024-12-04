package com.taosdata.flink;

import com.taosdata.jdbc.tmq.ReferenceDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;

public class ResultDeserializer extends ReferenceDeserializer<ResultBean> implements ResultTypeQueryable<ResultBean> {
    @Override
    public TypeInformation<ResultBean> getProducedType() {
        return  (TypeInformation.of(ResultBean.class));
    }
}
