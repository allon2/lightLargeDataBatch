/*
 * @(#)InstancePooledObjectFactory.java	1.0 2017年4月7日 上午12:49:15
 *
 * Copyright 2004-2010 Client Server International, Inc. All rights reserved.
 * CSII PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package io.wangsl.largedatabtach.pool;

import io.wangsl.largedatabtach.BatchDataConsumer;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class InstancePooledObjectFactory extends AbstractPooledObjectFactory {
    private BatchDataConsumer instance;
    private static Log log = LogFactory.getLog(AbstractPooledObjectFactory.class);
    public BatchDataConsumer getInstance() {
        return instance;
    }

    public void setInstance(BatchDataConsumer instance) {
        this.instance = instance;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool2.PooledObjectFactory#makeObject()
     */
    @Override
    public PooledObject<BatchDataConsumer> makeObject() throws Exception {
        BatchDataConsumer target = null;
        try {
            target = instance.getClass().newInstance();
        } catch (InstantiationException e) {
           log.error(e.getMessage());
        } catch (IllegalAccessException e) {
            log.error(e.getMessage());
        }

        org.springframework.beans.BeanUtils.copyProperties(instance, target);
        return new DefaultPooledObject((BatchDataConsumer) target);
    }


}
