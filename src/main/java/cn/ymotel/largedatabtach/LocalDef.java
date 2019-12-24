package cn.ymotel.largedatabtach;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.ObjectPool;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LocalDef {
    private ScheduledFuture scheduledFuture = null;
    /**
     * 定义每个批次的数据条数
     */
    private int batchsize = 0;
    private RunnableHelp runablehelp;
    private ObjectPool<BatchDataConsumer> pool = null;
    private List data = Collections.synchronizedList(new ArrayList());
    private Timestamp beginTime = new Timestamp(System.currentTimeMillis());

    public Timestamp getBeginTime() {
        return beginTime;
    }

    public long getLastUdateTime() {
        return lastUdateTime;
    }

    public void setLastUdateTime(long lastUdateTime) {
        this.lastUdateTime = lastUdateTime;
    }

    /**
     * 最近一次数据提交时间
     */

    private long lastUdateTime = System.currentTimeMillis();
    /**
     * 已经提交的数据量
     */
    private int bachedsize = 0;
    // 锁对象
    private Lock lock = new ReentrantLock();

    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    public ScheduledFuture getScheduledFuture() {
        return scheduledFuture;
    }

    public void setScheduledFuture(ScheduledFuture scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

    public int getBatchsize() {
        return batchsize;
    }

    public void setBatchsize(int batchsize) {
        this.batchsize = batchsize;
    }

    public RunnableHelp getRunablehelp() {
        return runablehelp;
    }

    public void setRunablehelp(RunnableHelp runablehelp) {
        this.runablehelp = runablehelp;
    }


    private Object poolkey=null;

    public Object getPoolkey() {
        return poolkey;
    }

    public void setPoolkey(Object poolkey) {
        this.poolkey = poolkey;
    }

    private KeyedObjectPool keyedPool;

    public KeyedObjectPool getKeyedPool() {
        return keyedPool;
    }

    public void setKeyedPool(KeyedObjectPool keyedPool) {
        this.keyedPool = keyedPool;
    }

    public List getData() {
        return data;
    }

    public void setData(List data) {
        this.data = data;
    }

    public int getBachedsize() {
        return bachedsize;
    }

    public void setBachedsize(int bachedsize) {
        this.bachedsize = bachedsize;
    }
}
