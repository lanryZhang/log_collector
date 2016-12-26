/*
* IProxy.java 
* Created on  202016/12/23 11:11 
* Copyright © 2012 Phoenix New Media Limited All Rights Reserved 
*/
package com.ifeng.logcollector.sinks;

/**
 * Class Description Here
 *
 * @author zhanglr
 * @version 1.0.1
 */
public interface IProxy {
    void append(String line);
    void flush();

    void start();

    int getAliveWorkCount();

    void close();
}
