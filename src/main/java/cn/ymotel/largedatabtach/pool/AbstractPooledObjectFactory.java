/*
 * @(#)InstancePooledObjectFactory.java	1.0 2017年4月7日 上午12:49:15
 *
 * Copyright 2004-2010 Client Server International, Inc. All rights reserved.
 * CSII PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package cn.ymotel.largedatabtach.pool;

import cn.ymotel.largedatabtach.BatchDataConsumer;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;


public abstract class AbstractPooledObjectFactory implements PooledObjectFactory<BatchDataConsumer> {

    /* (non-Javadoc)
     * @see org.apache.commons.pool2.PooledObjectFactory#destroyObject(org.apache.commons.pool2.PooledObject)
     */
    @Override
    public void destroyObject(PooledObject<BatchDataConsumer> p) throws Exception {
        BatchDataConsumer help = p.getObject();
        if (help != null) {
            help.close();
        }

    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool2.PooledObjectFactory#validateObject(org.apache.commons.pool2.PooledObject)
     */
    @Override
    public boolean validateObject(PooledObject<BatchDataConsumer> p) {

        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool2.PooledObjectFactory#activateObject(org.apache.commons.pool2.PooledObject)
     */
    @Override
    public void activateObject(PooledObject<BatchDataConsumer> p) throws Exception {
        BatchDataConsumer help = p.getObject();
        if (help != null) {
            help.setData(null);
        }

    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool2.PooledObjectFactory#passivateObject(org.apache.commons.pool2.PooledObject)
     */
    @Override
    public void passivateObject(PooledObject<BatchDataConsumer> p) throws Exception {
        BatchDataConsumer help = p.getObject();
        if (help != null) {
            help.setData(null);
        }

    }

}
