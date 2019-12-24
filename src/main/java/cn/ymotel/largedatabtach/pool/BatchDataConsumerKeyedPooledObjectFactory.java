package cn.ymotel.largedatabtach.pool;

import cn.ymotel.largedatabtach.BatchDataConsumer;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.springframework.context.ApplicationContext;

public class BatchDataConsumerKeyedPooledObjectFactory  implements KeyedPooledObjectFactory<Object, BatchDataConsumer> {
    private ApplicationContext springcontext;

    public ApplicationContext getSpringcontext() {
        return springcontext;
    }

    public void setSpringcontext(ApplicationContext springcontext) {
        this.springcontext = springcontext;
    }

    @Override
    public PooledObject<BatchDataConsumer> makeObject(Object key) throws Exception {
        if(springcontext!=null) {
            if (key instanceof String) {
                //spring bean
                return new DefaultPooledObject((BatchDataConsumer) springcontext.getBean((String) key));
            }
        }
        if(key instanceof  Class){
            return  new DefaultPooledObject((BatchDataConsumer)((Class)key).newInstance());

        }

        //普通类，直接进行复制
        BatchDataConsumer target = null;
        BatchDataConsumer instance=(BatchDataConsumer)key;
        try {
            target = instance.getClass().newInstance();
        } catch (InstantiationException e) {
            e.fillInStackTrace();
         } catch (IllegalAccessException e) {
            e.fillInStackTrace();
        }

        org.springframework.beans.BeanUtils.copyProperties(instance, target);


        return new DefaultPooledObject( target);
    }

    @Override
    public void destroyObject(Object key, PooledObject<BatchDataConsumer> p) throws Exception {
        BatchDataConsumer help = p.getObject();
        if (help != null) {
            help.close();
        }
    }

    @Override
    public boolean validateObject(Object key, PooledObject<BatchDataConsumer> p) {
        return false;
    }

    @Override
    public void activateObject(Object key, PooledObject<BatchDataConsumer> p) throws Exception {
        BatchDataConsumer help = p.getObject();
        if (help != null) {
            help.setData(null);
        }
    }

    @Override
    public void passivateObject(Object key, PooledObject<BatchDataConsumer> p) throws Exception {
        BatchDataConsumer help = p.getObject();
        if (help != null) {
            help.setData(null);
        }

    }
}
