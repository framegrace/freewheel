/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.framegrace.freewheel.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author marc
 */
public class Index extends ConcurrentSkipListMap<Row, Row> {
    String name="";
    String[] keys;
    boolean unique=true;
    
    public Index(String name,String[] keys,boolean unique) {
        super(new KeyValueComparator(keys));
        this.unique=unique;
        if (!unique) {
            ArrayList<String> s=new ArrayList<>(Arrays.asList(keys));
            s.add("s_oid");
            keys=s.toArray(keys);
        }
        this.keys=keys;
        this.name=name;
    }
    
    public String toString() {
        return name;
    }
}
