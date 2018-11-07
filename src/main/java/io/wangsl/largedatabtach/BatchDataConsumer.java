package io.wangsl.largedatabtach;

import java.io.Closeable;

/**
 *
 */
public interface BatchDataConsumer extends Runnable, Closeable {
    public void setData(Object obj);

}
