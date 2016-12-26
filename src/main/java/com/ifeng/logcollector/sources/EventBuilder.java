/*
* EventBuilder.java 
* Created on  202016/12/19 12:43 
* Copyright © 2012 Phoenix New Media Limited All Rights Reserved 
*/
package com.ifeng.logcollector.sources;

import org.apache.flume.source.avro.AvroFlumeEvent;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Class Description Here
 *
 * @author zhanglr
 * @version 1.0.1
 */
public class EventBuilder {
    public static AvroFlumeEvent wrap(Map header, ByteBuffer body){
        return new AvroFlumeEvent(header,body);
    }
    public static AvroFlumeEvent wrap(ByteBuffer body){
        return new AvroFlumeEvent(null,body);
    }
}
