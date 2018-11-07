package io.wangsl.largedatabtach.support;

import io.wangsl.largedatabtach.BatchDataConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;

import java.io.IOException;
import java.util.List;

public class MybatisBatchDataConsumer implements BatchDataConsumer {
    private SqlSessionFactory sqlSessionFactory;
    private Log log = LogFactory.getLog(MybatisBatchDataConsumer.class);

    @Override
    public void run() {
        getBatchSession();
        try {
            for (int i = 0; i < ls.size(); i++) {
                Object[] keys = (Object[]) ls.get(i);
                batchsqlSession.update((String) keys[0], keys[1]);
            }
            batchsqlSession.commit();

        } catch (Exception e) {
            log.error(e, e);
            batchsqlSession.rollback();

            /**
             * OpenSession 默认不提交，需要明示commit
             */
            org.apache.ibatis.session.SqlSession sqlSession = sqlSessionFactory.openSession();

            /**
             * 如果批量中有问题，进行单笔执行步骤
             */
            for (int i = 0; i < ls.size(); i++) {
                Object[] keys = (Object[]) ls.get(i);
                try {
                     sqlSession.update((String) keys[0], keys[1]);
                } catch (Exception e1) {
                    log.error("err----" + keys[0] + "---" + keys[1]);
                    log.error(e1, e1);
                }
            }
            sqlSession.commit();
            sqlSession.close();

        }
    }

    private org.apache.ibatis.session.SqlSession getBatchSession() {
        if (batchsqlSession == null) {
            batchsqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH);
        }
        return batchsqlSession;
    }

    public SqlSessionFactory getSqlSessionFactory() {
        return sqlSessionFactory;
    }

    org.apache.ibatis.session.SqlSession batchsqlSession = null;

    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;

    }

    private List ls = null;

    @Override
    public void setData(Object obj) {
        ls = (List) obj;

    }




    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {

        if (batchsqlSession != null) {
            batchsqlSession.close();
        }

    }

}
