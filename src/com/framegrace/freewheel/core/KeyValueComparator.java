/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.framegrace.freewheel.core;

import java.util.Comparator;

/**
 *
 * @author marc
 */
public class KeyValueComparator implements Comparator<Row> {
 
    String[] keys;
    
    public KeyValueComparator(String[] keys) {
        this.keys=keys;
    }

    @Override
    public int compare(Row o1, Row o2) {
        int c = 0;
        for (String key : keys) {
            Object k1 = o1.get(key);
            Object k2 = o2.get(key);
            if (k1 != null && k2 != null) {
                if (k1.getClass().equals(k1.getClass())) {
                    if (k1 instanceof Comparable) {
                        c = ((Comparable) k1).compareTo((Comparable) k2);
                    } else {
                        throw new RuntimeException("Uncomparable values " + k1 + " and " + k2);
                    }
                } else {
                    throw new RuntimeException("Trying to compare " + k1.getClass() + " and " + k2.getClass());
                }
                if (c != 0) {
                    return c;
                }
            } if (k1==null && k2==null) {
                c=0;
            } else if (k1==null) {
                return 1;
            } else if (k2==null) {
                return -1;
            }
        }
        return 0;
    }
}
