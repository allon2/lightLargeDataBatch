package io.wangsl.largedatabtach.support;

import io.wangsl.largedatabtach.LargeDataBatch;
import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;

public class MybatisResultHandler implements ResultHandler {
    private String sql;
    private LargeDataBatch databatch;

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setDatabatch(LargeDataBatch databatch) {
        this.databatch = databatch;
    }

    @Override
    public void handleResult(ResultContext resultContext) {
        databatch.addSql(sql,resultContext.getResultObject());
    }
}
