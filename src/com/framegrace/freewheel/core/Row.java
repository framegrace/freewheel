/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.framegrace.freewheel.core;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author marc
 */
public class Row extends ConcurrentSkipListMap<String, Object>{
    
    //static AtomicLong lastTime=new AtomicLong(System.currentTimeMillis()*10);
    static long lastTime=System.currentTimeMillis()*10;
    static final String lock="";
    
    public Row() {
        super();
        // Dubious precission enabler to allow unique timestamps
        // need someone with more fu on using Atomic and COS
         long time=System.currentTimeMillis()*10;
//        long newTime;
//        do { newTime=time+1; } while (lastTime.compareAndSet(newTime-1, newTime));

        synchronized(lock) {
            while (time<=lastTime) {time=time+1;}
            lastTime=time;
        }
        Long oid=utils.getUUID();        
        put("s_timestamp", time);
        put("s_oid", oid);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, Object> kp : descendingMap().entrySet()) {
            sb.append(kp.getKey());
            sb.append(":");
            sb.append(kp.getValue());
            sb.append(",");
        }
        return sb.toString();
    }
    
}
