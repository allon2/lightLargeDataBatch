package cn.ymotel.largedatabtach;

import cn.ymotel.largedatabtach.pool.InstancePooledObjectFactory;
import cn.ymotel.largedatabtach.pool.SpringBeanPoolFactory;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.List;
import java.util.concurrent.*;

/**
 * 实现多线程并发执行批量交易,交易数据由主线程顺序提供
 *
 * @author Administrator
 */
@ThreadSafe
public class AsycnLargeDataBatchHelp implements ApplicationContextAware, LargeDataBatch {
    private static Log log = LogFactory.getLog(AsycnLargeDataBatchHelp.class);

    public void setTotalThread(int totalThreadCount) {
        totalThreadsemaphore = new Semaphore(totalThreadCount, true);
    }

    /**
     * 内部调用，供ThreadSafeLargeDataBatchHelp使用
     * @param totalThreadsemaphore
     */
    protected void setTotalThreadsemaphore(Semaphore totalThreadsemaphore) {
        this.totalThreadsemaphore = totalThreadsemaphore;
    }

    private Semaphore totalThreadsemaphore = null;


    /**
     * 保持个线程的注释
     */
    private final LocalDef def = new LocalDef();


    private ApplicationContext context;


    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param beanName   在配置文件中改Aciton 需配置为singleton="false"
     */
    @Override
    public void init(int batchsize, int threadsize, String beanName) {
        SpringBeanPoolFactory factory = new SpringBeanPoolFactory();
        factory.setSpringcontext(context);
        factory.setBeanName(beanName);
        innerInit(batchsize, threadsize, factory);
    }

    private  ScheduledExecutorService scheduleservice = Executors.newSingleThreadScheduledExecutor();



    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param beanName   在配置文件中改Aciton 需配置为singleton="false"
     * @param timeout    超过时间，线程会将未达阀值数据，自动提交
     */
    @Override
    public void init(int batchsize, int threadsize, String beanName, long timeout) {
        init(batchsize, threadsize, beanName);
        initFixDelaySchedule(timeout);
    }

    private void initFixDelaySchedule(final long timeout) {

        ScheduledFuture scheduledFuture = scheduleservice.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {

                if ((System.currentTimeMillis() - def.getLastUdateTime()) > timeout) {
                    try {
                        UpdateBatch(def, false, false);
                    } catch (Exception e) {
                        log.error("scheduleFix--", e);
                    }
                }

            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);
        def.setScheduledFuture(scheduledFuture);
    }

    @Override
    public void init(int batchsize, int threadsize, BatchDataConsumer t) {
        InstancePooledObjectFactory factory = new InstancePooledObjectFactory();
        factory.setInstance(t);
        innerInit(batchsize, threadsize, factory);
    }

    @Override
    public void init(int batchsize, int threadsize, BatchDataConsumer t, long timeout) {
        init(batchsize, threadsize, t);
        initFixDelaySchedule(timeout);
    }

    public  ScheduledExecutorService getScheduleservice() {
        return scheduleservice;
    }

    public  void setScheduleservice(ScheduledExecutorService scheduleservice) {
        this.scheduleservice = scheduleservice;
    }

    private void innerInit(int batchsize, int threadsize, PooledObjectFactory<BatchDataConsumer> pooledObjectFactory) {
        def.setBatchsize(batchsize);
        def.setRunablehelp(new RunnableHelp(threadsize));
        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
        conf.setMaxTotal(threadsize);
        ObjectPool<BatchDataConsumer> pool = new GenericObjectPool<BatchDataConsumer>(pooledObjectFactory, conf);
//        def.setPool(pool);
    }

    @Override
    public void addSql(String sql, Object obj){
        Object[] keys = new Object[2];
        keys[0] = sql;
        keys[1] = obj;
        addData(keys);
    }


    @Override
    public void addData(Object obj) {
        List ls = def.getData();
        ls.add(obj);
        if (ls.size() >= def.getBatchsize()) {
            UpdateBatch(false);

        }
    }


    /**
     * 重复使用线程
     *
     * @return
     */
    private BatchDataConsumer getConsumer(LocalDef def) {
//        ObjectPool<BatchDataConsumer> pool = def.getPool();
//        if (pool != null) {
//            try {
//                return pool.borrowObject();
//            } catch (Exception e) {
//             log.error(e.getMessage());
//
//            }
//        }
//        return null;
        return null;
    }

    private void UpdateBatch(boolean isEnd) {
        UpdateBatch(def, isEnd, true);
    }

    private void UpdateBatch(LocalDef def, boolean isEnd, boolean waitForSubmitData) {
        List ls = def.getData();

        if (ls == null || ls.isEmpty()) {
            return;
        }
        if (totalThreadsemaphore != null) {
            if (waitForSubmitData) {
                try {
                    totalThreadsemaphore.acquire();
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                    Thread.currentThread().interrupt();
                }
            } else {
                if (totalThreadsemaphore.tryAcquire()) {
                } else {
                    return;
                }

            }

        }


        BatchDataConsumer bean = getConsumer(def);





        RunnableHelp runnableHelp = def.getRunablehelp();
        /**
         * 在此发生阻塞
         */
        runnableHelp.addRunable( bean, totalThreadsemaphore, waitForSubmitData, def);
        log.info("batch--" + def.getBachedsize());
    }

    @Override
    public void end() {
        UpdateBatch(true);

        ScheduledFuture scheduledFuture = def.getScheduledFuture();
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        RunnableHelp runnableHelp = def.getRunablehelp();
        runnableHelp.end();


        log.info("batch--end" + def.getBachedsize());

    }

    @Override
    public void setApplicationContext(ApplicationContext arg0)
    {
        context = arg0;
    }

    public   void shutdown(){

        if(scheduleservice!=null&&(!scheduleservice.isShutdown())) {
            try {
                scheduleservice.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
