/*
* KafkaProxy.java 
* Created on  202016/12/19 12:42 
* Copyright © 2012 Phoenix New Media Limited All Rights Reserved 
*/
package com.ifeng.logcollector.sinks;


import com.ifeng.kafka.exceptions.EmptyWorkThreadException;
import com.ifeng.kafka.producer.KafkaProducerEx;
import com.ifeng.kafka.producer.ProducerFactory;
import com.ifeng.logcollector.sources.SpoolDirSource;
import org.apache.log4j.Logger;

/**
 * Class Description Here
 *
 * @author zhanglr
 * @version 1.0.1
 */
public class KafkaProxy implements IProxy {

    private KafkaProducerEx proxy;
    private String basePath;
    private String topic;
    protected Logger logger = Logger.getLogger(SpoolDirSource.class);

    public KafkaProxy(String basePath,String topic){
        proxy = ProducerFactory.getBacthInstnace(basePath,topic);
    }

    @Override
    public void append(String line) {
        proxy.sendBatch(line);
    }

    @Override
    public void flush() {
        proxy.flush();
    }

    @Override
    public void start(){proxy.start();}

    @Override
    public int getAliveWorkCount() {
        return proxy.getAliveWorkCount();
    }
    @Override
    public void close() {
        proxy.close();
    }
}
