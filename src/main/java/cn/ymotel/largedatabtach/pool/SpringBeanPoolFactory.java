/*
 * @(#)SpringBeanPoolFactory.java	1.0 2017年4月7日 上午12:31:51
 *
 * Copyright 2004-2010 Client Server International, Inc. All rights reserved.
 * CSII PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package cn.ymotel.largedatabtach.pool;

import cn.ymotel.largedatabtach.BatchDataConsumer;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.springframework.context.ApplicationContext;


public class SpringBeanPoolFactory extends AbstractPooledObjectFactory {
    private ApplicationContext springcontext;

    public ApplicationContext getSpringcontext() {
        return springcontext;
    }

    public void setSpringcontext(ApplicationContext springcontext) {
        this.springcontext = springcontext;
    }

    private String beanName;


    public String getBeanName() {
        return beanName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool2.PooledObjectFactory#makeObject()
     */
    @Override
    public PooledObject<BatchDataConsumer> makeObject() throws Exception {
        return new DefaultPooledObject((BatchDataConsumer) springcontext.getBean(beanName));
    }


}
