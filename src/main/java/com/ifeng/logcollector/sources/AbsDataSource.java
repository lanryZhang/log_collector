/*
* AbsDataSource.java 
* Created on  202016/12/23 11:07 
* Copyright © 2012 Phoenix New Media Limited All Rights Reserved 
*/
package com.ifeng.logcollector.sources;

import com.ifeng.configurable.Configurable;
import com.ifeng.configurable.Context;
import com.ifeng.logcollector.sinks.AvroProxy;
import com.ifeng.logcollector.sinks.IProxy;
import com.ifeng.logcollector.sinks.KafkaProxy;
import org.apache.log4j.Logger;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class Description Here
 *
 * @author zhanglr
 * @version 1.0.1
 */
public abstract class AbsDataSource implements Configurable {
    protected  String basePath ;
    protected Lock lock = new ReentrantLock();
    protected Condition condition = lock.newCondition();
    protected String completedFilePath;
    protected int batchSize = 2000;
    protected String hostIp = "0.0.0.0";
    protected int port = 80;
    protected String topic;
    protected boolean restart = true;
    protected boolean writeFileName = false;
    protected Boolean sync = true;
    protected Logger logger = Logger.getLogger(SpoolDirSource.class);
    protected IProxy proxy;
    protected String sinkType;
    protected String localIp = null;
    protected String charset = "utf-8";
    protected  String[] pathExpression;
    protected long lastFlushTime = System.currentTimeMillis();
    protected long batchTimeout = 30 * 1000;

    protected void initProxy(){
        int reconnectCnt = 0;
        try{
            switch (sinkType.toLowerCase()){
                case "avro":
                    proxy = new AvroProxy(hostIp, port, batchSize, topic, batchTimeout,sync);
                    break;
                case "kafka":
                    proxy = new KafkaProxy(basePath,topic);
                    break;
                default:
                    throw new Exception("no sinks config");
            }
        }catch (Exception e){
            logger.error(e);
            logger.error("reconnect :" + ++reconnectCnt);
            initProxy();
            try {
                Thread.currentThread().sleep(5 * 1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }finally {
            logger.info("initialize proxy success!");
        }
    }
    @Override
    public void config(Context context) {
        basePath = context.getString("basePath","");
        completedFilePath  = context.getString("completedFilePath");
        hostIp = context.getString("hostIp");
        port = context.getInt("port");
        batchSize = context.getInt("batchSize");
        topic = context.getString("topic");
        writeFileName = Boolean.valueOf(context.getString("writeFileName","false"));
        batchTimeout = context.getLong("batchTimeout",10*1000L);
        sync = context.getBoolean("sync","true");
        localIp = context.getString("localIp");
        localIp = context.getString("localIp");
        sinkType = context.getString("sink.type","");
        pathExpression = context.getString("pathExpression","").split(",");
    }

}
