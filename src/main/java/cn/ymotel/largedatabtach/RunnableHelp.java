package cn.ymotel.largedatabtach;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
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


    public RunnableHelp(int poolsize, ExecutorService executors) {
        super();
//		executors=Executors.newFixedThreadPool(poolsize);
        this.executors = executors;
        semaphore = new Semaphore(poolsize, true);
    }

    /**
     * @param command
     * @param totalsemaphore
     * @param waitForSubmitData 是否等待锁并提交数据，如果队列已满，对于定时任务应该忽略此处提交的相关数据，等待下次轮训，不应该一直等待 true 等待提交，false不等待提交，返回
     */
    public void addRunable(final BatchDataConsumer command, final Semaphore totalsemaphore, boolean waitForSubmitData, final LocalDef def) {
        try {


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

            List ls = def.getData();
            if (ls == null || ls.isEmpty()) {
                return;
            }
            def.getLock().lock();
            command.setData(ls);
            def.setData(new ArrayList());
            def.setLastUdateTime(System.currentTimeMillis());
            def.setBachedsize(def.getBachedsize() + ls.size());
            log.info("batch--" + def.getBachedsize());
            def.getLock().unlock();

            try {
                executors.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            command.run();
                        } finally {
                            try {
                                def.getPool().returnObject(command);
                            } catch (Exception e) {
                                log.error(e.getMessage());
                            }
                            semaphore.release();
                            if (totalsemaphore != null) {
                                totalsemaphore.release();
                            }
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                semaphore.release();
                if (totalsemaphore != null) {
                    totalsemaphore.release();
                }
                throw e;
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void end() {
        executors.shutdown();
        try {
            boolean loop = true;
            do {    //等待所有任务完成
                loop = !executors.awaitTermination(2, TimeUnit.SECONDS);
            } while (loop);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }

    }
}
