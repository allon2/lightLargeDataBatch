package cn.ymotel.largedatabtach;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class RunnableHelp {
    private static Log log = LogFactory.getLog(RunnableHelp.class);

    private Semaphore semaphore;

    public Semaphore getSemaphore() {
        return semaphore;
    }

    private ExecutorService executors = null;
    private  int poolsize=0;

//    private static ObjectPool runablepool=null;
//    static{
//        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
//        conf.setMaxTotal(Integer.MAX_VALUE);
//        ObjectPool<BatchDataConsumer> pool = new GenericObjectPool<BatchDataConsumer>(pooledObjectFactory, conf);
//
//    }

    public RunnableHelp(int poolsize, ExecutorService executors) {
        super();
        this.poolsize=poolsize;
//		executors=Executors.newFixedThreadPool(poolsize);
        this.executors = executors;
        semaphore = new Semaphore(poolsize, true);
    }
    public RunnableHelp(int poolsize) {
        this(poolsize,null);
    }

    /**
     * @param command
     * @param totalsemaphore
     * @param waitForSubmitData 是否等待锁并提交数据，如果队列已满，对于定时任务应该忽略此处提交的相关数据，等待下次轮训，不应该一直等待 true 等待提交，false不等待提交，返回
     */
    public void addRunable(final BatchDataConsumer command, final Semaphore totalsemaphore, boolean waitForSubmitData, final LocalDef def) {
        try {
            List ls = def.getData();
            if (ls == null || ls.isEmpty()) {
                return;
            }
            if(command==null){
                return ;
            }

            if (waitForSubmitData) {
                /**
                 * 通过此地方阻塞当前线程，防止有新数据提交
                 */
                semaphore.acquire();
            } else {
                /**
                 * 定时提交走此代码，
                 */
                if (semaphore.tryAcquire()) {

                } else {

                    return;
                }

            }

            /**
             * 执行数据交换,将代码放在此处是因为目前当前线程和定时线程都有可能执行此代码，
             *  定时交易中，只有有可用线程，才能将数据放到队列中，为了保证数据交换的原子性。
             *  只有取得数据才能将数据放到线程池中执行。
             *  def.getData中的数据大于0的情况下，当前线程是可以往里面继续放入数据的，所以可能在执行信号量请求时数据为1，在command.setData时数据量已经发生改变，变为2，中间有个时间差，目前此时间应该极少，暂时不考虑
             */

            if(executors!=null) {
                def.getLock().lock();
            }
            command.setData(ls);
            def.setData(Collections.synchronizedList(new ArrayList()));
            def.setLastUdateTime(System.currentTimeMillis());
            def.setBachedsize(def.getBachedsize() + ls.size());
            log.debug("batch--" + def.getBachedsize());
            if(executors!=null) {

                def.getLock().unlock();
            }
            WrapBatchDataConsumerRunnable runnable= (WrapBatchDataConsumerRunnable)def.getKeyedPool().borrowObject(WrapBatchDataConsumerRunnable.class);
            runnable.setCommand(command);
            runnable.setTotalsemaphore(totalsemaphore);
            runnable.setDef(def);
            runnable.setSemaphore(semaphore);
            runnable.setKeyedPool(def.getKeyedPool());


            if(executors==null){
                runnable.run();
//                CommandRun(command,totalsemaphore,def);
            }else {

                try {
                    executors.execute(runnable);
//                    executors.execute(new Runnable() {
//                        @Override
//                        public void run() {
//                            CommandRun(command,totalsemaphore,def);
//                        }
//                    });
                } catch (RejectedExecutionException e) {
                    semaphore.release();
                    if (totalsemaphore != null) {
                        totalsemaphore.release();
                    }
                    throw e;
                }
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//    private void  CommandRun(BatchDataConsumer command,Semaphore totalsemaphore,LocalDef def ){
//        try {
//            command.run();
//        } finally {
//            try {
//                def.getKeyedPool().returnObject(def.getPoolkey(), command);
//            } catch (java.lang.Throwable e) {
//
//                log.error(e.getMessage());
//            }
//            semaphore.release();
//            if (totalsemaphore != null) {
//                totalsemaphore.release();
//            }
//        }
//    }
    public void end() {
        try {
            semaphore.acquire(this.poolsize);
            semaphore.release(this.poolsize);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        executors.shutdown();
//        try {
//            boolean loop = true;
//            do {    //等待所有任务完成
//                loop = !executors.awaitTermination(2, TimeUnit.SECONDS);
//            } while (loop);
//        } catch (InterruptedException e) {
//            log.error(e.getMessage());
//            Thread.currentThread().interrupt();
//        }

    }
}
