package cn.ymotel.largedatabtach;

import org.apache.commons.pool2.KeyedObjectPool;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class WrapBatchDataConsumerRunnable implements BatchDataConsumer {
    private BatchDataConsumer command;
    private Semaphore totalsemaphore;
    private Semaphore semaphore;
    private LocalDef def;
    private  int semaphoresize=0;

   private  KeyedObjectPool keyedPool;



    public void setKeyedPool(KeyedObjectPool keyedPool) {
        this.keyedPool = keyedPool;
    }


    public void setCommand(BatchDataConsumer command) {
        this.command = command;
    }


    public void setTotalsemaphore(Semaphore totalsemaphore) {
        this.totalsemaphore = totalsemaphore;
    }


    public void setSemaphore(Semaphore semaphore) {
        this.semaphore = semaphore;
    }


    public void setDef(LocalDef def) {
        this.def = def;
    }
//    public static void main(String[] args){
//        WrapBatchDataConsumerRunnable run=new WrapBatchDataConsumerRunnable();
//        WrapBatchDataConsumerRunnable run1=new WrapBatchDataConsumerRunnable();
//        System.out.println(run.getClass()==run1.getClass());
//    }
    @Override
    public void run() {
        try {
            command.run();
        } finally {
            try {
                def.getKeyedPool().returnObject(def.getPoolkey(), command);
            } catch (java.lang.Throwable e) {

               e.fillInStackTrace();
            }
            if(semaphore!=null) {
                semaphore.release();
            }
             if (totalsemaphore != null) {
                totalsemaphore.release();
            }
            try {
                keyedPool.returnObject(this.getClass(),this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void setData(Object obj) {

    }

    @Override
    public void close() throws IOException {

    }
}
