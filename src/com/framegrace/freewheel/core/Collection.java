/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.framegrace.freewheel.core;

import com.vladium.utils.IObjectProfileNode;
import com.vladium.utils.ObjectProfiler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author marc
 */
public class Collection implements Comparable<Collection>{

    
    String name;
    String[] main_index_fields={"s_oid","s_timestamp"};
    String[] session_index_fields={"s_sid","s_oid"};
    Index main_index;
    Index session_index;
    ConcurrentHashMap<String,Index> indexes=new ConcurrentHashMap<>();
    
    public Collection(String name) {
        this.name=name;
        // Create base index
        main_index=new Index("main",main_index_fields,true);
        session_index=new Index("session",session_index_fields,true);
        indexes.put(main_index.name,main_index);
        indexes.put(session_index.name,session_index);
    }
    
    public void createIndex(String name,String[] keys,boolean unique) throws FWException {
        // Check what happens to not commited data
        Index i=new Index(name,keys,unique);
        for(String k:keys) { System.out.print(k); };System.out.println();
        System.out.println("===== Indexing "+name+" ====");
        int j=0;
        for(Row kv:main_index.keySet()) {
            j++;
            Row k=i.put(kv, kv);
            if (k!=null) throw new FWException("Duplicate keys");
        }
        System.out.println("===== "+j+" keys indexed ====");
        indexes.put(i.name,i);
    }
    
    public static void main(String[] args) {
        try {
            Session mysession=new Session();
           IObjectProfileNode profile = ObjectProfiler.profile (mysession);
            System.out.println ("obj size = " + profile.size () + " bytes");
            Collection test=new Collection("Test");
            Row p=new Row();
            p.put("Test", "Data4");
            mysession.insert(test,p);
            Row p1=new Row();
            p1.put("Test", "Data3");
            mysession.insert(test,p1);
            p=new Row();
            p.put("Test", "Data1");
            mysession.insert(test,p);
            p=new Row();
            p.put("Test", "Data1");
            mysession.insert(test,p);
            Row from=new Row();
            Row to=new Row();
            from.put("s_oid", Long.MIN_VALUE);
            from.put("s_timestamp",0L);
            to.put("s_oid", Long.MAX_VALUE);
            to.put("s_timestamp",System.currentTimeMillis()*10);
            for (Iterator<Row> it = mysession.get(test,from,to,"main",false).iterator(); it.hasNext();) {
                Row kp = it.next();
                System.out.println(kp);
            }
            mysession.commit();
            System.out.println("=============================");
            for (Iterator<Row> it = mysession.get(test,from,to,"main",false).iterator(); it.hasNext();) {
                Row kp = it.next();
                System.out.println(kp);
            }
            System.out.println("========== By Test ================");
            test.createIndex("ByTest",new String[] {"Test"},false);
            for (Iterator<Row> it = test.indexes.get("ByTest").keySet().iterator(); it.hasNext();) {
                Row kp = it.next();
                System.out.println(kp);
            }
        } catch (FWException ex) {
            Logger.getLogger(Collection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int compareTo(Collection o) {
        return name.compareTo(o.name);
    }
    
}
