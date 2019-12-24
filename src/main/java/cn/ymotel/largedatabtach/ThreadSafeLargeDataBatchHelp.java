package cn.ymotel.largedatabtach;


import cn.ymotel.largedatabtach.pool.BatchDataConsumerKeyedPooledObjectFactory;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;


/**
 * 实现多线程并发执行批量交易,交易数据由主线程顺序提供
 *
 * @author Administrator
 */
@ThreadSafe
public class ThreadSafeLargeDataBatchHelp implements ApplicationContextAware,LargeDataBatch, InitializingBean {
    /**
     * 设置在一个jvm内的总的最高可并行的线程数,在一个jvm中可能会多个地方调用ThreadSafeLargeDataBatchHelp类。
     * 如果不加控制，可能导致应用服务器在同一时间线程数过大,应用服务器处理异常情况发生，如果消费者同时超过数据库也可能导致数据库超过阈值情况发生
     * 不设置,则不会对线程池中的线程数进行总体控制
     * @param totalThreadCount
     */
    public void setTotalThread(int totalThreadCount) {
        totalThreadsemaphore = new Semaphore(totalThreadCount, true);
    }

    private Semaphore totalThreadsemaphore = null;

    private static ThreadLocal<LargeDataBatchHelp> threadHelp=new ThreadLocal<LargeDataBatchHelp>(){
        @Override
        protected synchronized LargeDataBatchHelp initialValue() {
             return new LargeDataBatchHelp();
        }
    };
    private  java.util.concurrent.ScheduledExecutorService scheduleservice = Executors.newSingleThreadScheduledExecutor();
    private ExecutorService threadpool=Executors.newCachedThreadPool();

    public ScheduledExecutorService getScheduleservice() {
        return scheduleservice;
    }

    public void setScheduleservice(ScheduledExecutorService scheduleservice) {
        this.scheduleservice = scheduleservice;
    }

    public ExecutorService getThreadpool() {
        return threadpool;
    }

    public void setThreadpool(ExecutorService threadpool) {
        this.threadpool = threadpool;
    }

    private ApplicationContext context;
    private KeyedObjectPool<Object,BatchDataConsumer>  keyedPool;


    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param beanName   在配置文件中改Aciton 需配置为singleton="false"
     */
    public void init(int batchsize, int threadsize, String beanName) {
        LargeDataBatchHelp help= threadHelp.get();
        if(context!=null){
            help.setApplicationContext(context);
        }
        help.setKeyedPool(keyedPool);

        help.setTotalThreadsemaphore(totalThreadsemaphore);
        if(scheduleservice!=null){
            help.setScheduleservice(scheduleservice);
        }
        if(threadpool!=null){
            help.setThreadpool(threadpool);
        }
        help.init(batchsize,threadsize,beanName);

    }


    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param beanName   在配置文件中改Aciton 需配置为singleton="false"
     * @param timeout    超过时间，线程会将未达阀值数据，自动提交
     */
    public void init(int batchsize, int threadsize, String beanName, long timeout) {
        LargeDataBatchHelp help= threadHelp.get();
        help.setKeyedPool(keyedPool);

        if(context!=null){
            help.setApplicationContext(context);
        }
        if(scheduleservice!=null){
            help.setScheduleservice(scheduleservice);
        }
        if(threadpool!=null){
            help.setThreadpool(threadpool);
        }
        help.init(batchsize,threadsize,beanName,timeout);

    }


    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param t   实现 BatchDataInterface 的对象
     */
    public void init(int batchsize, int threadsize, BatchDataConsumer t) {
        LargeDataBatchHelp help= threadHelp.get();
        if(scheduleservice!=null){
            help.setScheduleservice(scheduleservice);
        }
        if(threadpool!=null){
            help.setThreadpool(threadpool);
        }
        help.setKeyedPool(keyedPool);
        help.init(batchsize,threadsize,t);


    }
    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param t   实现 BatchDataInterface 的对象
     * @param timeout    超过时间，线程会将未达阀值数据，自动提交
     */
    public void init(int batchsize, int threadsize, BatchDataConsumer t, long timeout) {
        LargeDataBatchHelp help= threadHelp.get();
        if(scheduleservice!=null){
            help.setScheduleservice(scheduleservice);
        }
        if(threadpool!=null){
            help.setThreadpool(threadpool);
        }
        help.setKeyedPool(keyedPool);
        help.init(batchsize,threadsize,t,timeout);

    }

    /**
     * 程序会将sql和obj数据组成一个数组,放入队列中，在达到阈值将数据放入List中供消费者调用
     * @param sql 准备处理的sql语句
     * @param obj 准备处理的数据
     */
    public void addSql(String sql, Object obj)  {
        Object[] keys = new Object[2];
        keys[0] = sql;
        keys[1] = obj;
        addData(keys);
    }
    /**
     * 程序会将obj,放入队列中，在达到阈值将数据放入List中供消费者调用
     * @param obj 准备处理的数据
     */
    public void addData(Object obj){
        LargeDataBatch help= threadHelp.get();
        help.addData(obj);
    }

    /**
     * 数据处理结尾一般会剩余一些未达到batchsize的数据未处理,通过调用此方法,可将未达到阈值的数据提交到线程池中进行处理
     */
    public void end() {
        LargeDataBatch help= threadHelp.get();
        help.end();
        threadHelp.remove();
    }

//    @Override
    public void shutdown() {
        keyedPool.close();

        if(threadpool!=null&&(!threadpool.isShutdown())) {

            try {
                threadpool.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if(scheduleservice!=null&&(!scheduleservice.isShutdown())) {
            try {
                scheduleservice.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext arg0) {
        context = arg0;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if(keyedPool==null) {
            GenericKeyedObjectPoolConfig genericKeyedObjectPoolConfig = new GenericKeyedObjectPoolConfig();
            if (totalThreadsemaphore != null) {
                genericKeyedObjectPoolConfig.setMaxTotalPerKey(totalThreadsemaphore.availablePermits());
            } else {
                genericKeyedObjectPoolConfig.setMaxTotalPerKey(Integer.MAX_VALUE);
            }
            BatchDataConsumerKeyedPooledObjectFactory batchDataConsumerKeyedPooledObjectFactory = new BatchDataConsumerKeyedPooledObjectFactory();
            batchDataConsumerKeyedPooledObjectFactory.setSpringcontext(this.context);
            keyedPool = new GenericKeyedObjectPool<Object, BatchDataConsumer>(batchDataConsumerKeyedPooledObjectFactory);
        }

    }
}
