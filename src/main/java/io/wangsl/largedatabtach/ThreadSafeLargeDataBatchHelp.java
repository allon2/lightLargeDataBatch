package io.wangsl.largedatabtach;


import net.jcip.annotations.ThreadSafe;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.concurrent.Semaphore;


/**
 * 实现多线程并发执行批量交易,交易数据由主线程顺序提供
 *
 * @author Administrator
 */
@ThreadSafe
public class ThreadSafeLargeDataBatchHelp implements ApplicationContextAware,LargeDataBatch {
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

    private ThreadLocal<LargeDataBatchHelp> threadHelp=new ThreadLocal<LargeDataBatchHelp>(){
        @Override
        protected synchronized LargeDataBatchHelp initialValue() {
            return new LargeDataBatchHelp();
        }
    };

    private ApplicationContext context;


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
        help.init(batchsize,threadsize,beanName);
        help.setTotalThreadsemaphore(totalThreadsemaphore);
    }


    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param beanName   在配置文件中改Aciton 需配置为singleton="false"
     * @param timeout    超过时间，线程会将未达阀值数据，自动提交
     */
    public void init(int batchsize, int threadsize, String beanName, long timeout) {
        LargeDataBatchHelp help= threadHelp.get();
        if(context!=null){
            help.setApplicationContext(context);
        }
        help.init(batchsize,threadsize,beanName,timeout);
    }


    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param t   实现 BatchDataInterface 的对象
     */
    public void init(int batchsize, int threadsize, BatchDataConsumer t) {
        LargeDataBatch help= threadHelp.get();
        help.init(batchsize,threadsize,t);
    }
    /**
     * @param batchsize  在thread中的每次执行条数
     * @param threadsize 同时并发的线程数
     * @param t   实现 BatchDataInterface 的对象
     * @param timeout    超过时间，线程会将未达阀值数据，自动提交
     */
    public void init(int batchsize, int threadsize, BatchDataConsumer t, long timeout) {
        LargeDataBatch help= threadHelp.get();
        help.init(batchsize,threadsize,t,timeout);
    }

    /**
     * 程序会将sql和obj数据组成一个数组,放入队列中，在达到阈值将数据放入List中供消费者调用
     * @param sql
     * @param obj
     */
    public void addSql(String sql, Object obj)  {
        Object[] keys = new Object[2];
        keys[0] = sql;
        keys[1] = obj;
        addData(keys);
    }
    /**
     * 程序会将obj,放入队列中，在达到阈值将数据放入List中供消费者调用
     * @param obj
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
    }

    @Override
    public void setApplicationContext(ApplicationContext arg0) {
        context = arg0;
    }

}
