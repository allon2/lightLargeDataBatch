package cn.ymotel.largedatabtach;

public interface LargeDataBatch {
    void init(int batchsize, int threadsize, String beanName);

    void init(int batchsize, int threadsize, String beanName, long timeout);

    void init(int batchsize, int threadsize, BatchDataConsumer t);

    void init(int batchsize, int threadsize, BatchDataConsumer t, long timeout);

    void addSql(String sql, Object obj);

    void addData(Object obj);

    void end();

}
