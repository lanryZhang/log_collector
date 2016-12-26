/*
* ExpireMap.java 
* Created on  202016/11/15 11:55 
* Copyright © 2012 Phoenix New Media Limited All Rights Reserved 
*/
package com.ifeng.logcollector.core;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Class Description Here
 *
 * @author zhanglr
 * @version 1.0.1
 */
public class ExpireMap<K,V>{

    private ConcurrentHashMap<K,V> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<K,Long> expreMap = new ConcurrentHashMap<>();
    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
    public ExpireMap(){
        executorService.scheduleAtFixedRate(new ClearExpire(),1000,1000,TimeUnit.MILLISECONDS);
    }
    class ClearExpire implements Runnable{

        @Override
        public void run() {
            try {
                for (Map.Entry<K,Long> en: expreMap.entrySet()
                     ) {
                    if (System.currentTimeMillis() >= en.getValue()) {
                        map.remove(en.getKey());
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void put(K key,V value,Long expireAt){
        map.put(key,value);
        expreMap.put(key,expireAt);
    }

    public V get(K key){
        return map.get(key);
    }
}
