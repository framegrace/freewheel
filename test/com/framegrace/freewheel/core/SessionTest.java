/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.framegrace.freewheel.core;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author marc
 */
public class SessionTest {
    
    Collection testCol=new Collection("testCollection");
    Long[] oids=new Long[10];
    
    public SessionTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        //Setup collection with some sample data
        System.out.println("Setting up");
        try {
            testCol.createIndex("by text",new String[] {"text"}, false);
        } catch (FWException ex) {
            Logger.getLogger(SessionTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        Session setupSession=new Session();
        for (int i = 0; i < 10; i++) {
            try {
                Row r = new Row();
                r.put("code", (i*10));
                r.put("text", "Setup Row "+(i*10));
                setupSession.insert(testCol, r);
                oids[i]=(Long)r.get("s_oid");
            } catch (FWException ex) {
                Logger.getLogger(SessionTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        setupSession.commit();
        //printAll();
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of get method, of class Session.
     */
    @Test
    public void testGet_Collection_long() {
        System.out.println("get by oid");
        Session instance = new Session();
        for (int i = 0; i < 10; i++) {
            Row result = instance.get(testCol, oids[i]);
            assertNotNull(result);
            String data=(String)result.get("text");
            assertEquals("Setup Row "+(i*10), data);
        }
    }

    /**
     * Test of get method, of class Session.
     */
    @Test
    public void testGet_5args() {
        System.out.println("get from/to");
        Row from = new Row();
        Row to = new Row();
        from.put("s_oid", Long.MIN_VALUE);
        from.put("s_timestamp", 0L);
        to.put("s_oid", Long.MAX_VALUE);
        to.put("s_timestamp", System.currentTimeMillis() * 10);
        String index = "main";
        Session instance = new Session();
        int i=0;
        System.out.println("Check main index");
        for (Iterator<Row> it = instance.get(testCol, from, to, "main", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            i=i+1;
        }
        assertEquals(10, i);
        i=0;
        System.out.println("Check 'text' Index Full asc");
        from = new Row();
        to = new Row();
        from.put("text", "");
        to.put("text","zzzzzzzzzzzzzzzzzzzzzzzzzz");
        int last_code=-1;
        for (Iterator<Row> it = instance.get(testCol, from, to, "by text", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            int code=(Integer)kp.get("code");
            assertTrue(code>last_code);
            last_code=code;
            i=i+1;
        }
        System.out.println("Check 'text' Index Full desc");
        i=0;
        last_code=100;
        for (Iterator<Row> it = instance.get(testCol, from, to, "by text", true).iterator(); it.hasNext();) {
            Row kp = it.next();
            int code=(Integer)kp.get("code");
            assertTrue(code<last_code);
            last_code=code;
            i=i+1;
        }
        System.out.println("Check 'text' Index partial");
        from.put("text", "Setup Row 20");
        to.put("text","Setup Row 70");
        last_code=1;
        i=0;
        for (Iterator<Row> it = instance.get(testCol, from, to, "by text", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            int code=(Integer)kp.get("code");
            assertTrue(code>last_code);
            last_code=code;
            i=i+1;
        }
        assertEquals(6, i);
        System.out.println("Check Session Isolation");
        Session s2 = new Session();
        Row r2 = new Row();
        r2.put("code", 35);
        r2.put("text", "Setup Row 35");
        try {
            s2.insert(testCol,r2);
        } catch (FWException ex) {
            Logger.getLogger(SessionTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        last_code=1;
        i=0;
        for (Iterator<Row> it = instance.get(testCol, from, to, "by text", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            int code=(Integer)kp.get("code");
            assertTrue(code>last_code);
            last_code=code;
            i=i+1;
        }
        assertEquals(6, i);
        last_code=1;
        i=0;
        for (Iterator<Row> it = s2.get(testCol, from, to, "by text", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            int code=(Integer)kp.get("code");
            assertTrue(code>last_code);
            last_code=code;
            i=i+1;
        }
        assertEquals("ep",7, i);
        s2.commit();
        last_code=1;
        i=0;
        for (Iterator<Row> it = instance.get(testCol, from, to, "by text", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            int code=(Integer)kp.get("code");
            assertTrue(code>last_code);
            last_code=code;
            i=i+1;
        }
        assertEquals(7, i);
    }

    /**
     * Test of insert method, of class Session.
     */
    @Test
    public void testInsert() throws Exception {
        System.out.println("insert");
        Collection c = null;
        Row kv = null;
        Session instance = new Session();
        instance.insert(c, kv);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of delete method, of class Session.
     */
    @Test
    public void testDelete() {
        System.out.println("delete");
        Collection c = null;
        Row kv = null;
        Index i = null;
        Session instance = new Session();
        instance.delete(c, kv, i);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of put method, of class Session.
     */
    @Test
    public void testPut() throws Exception {
        System.out.println("put");
        Collection c = null;
        Index index = null;
        Row kv = null;
        Session instance = new Session();
        Row expResult = null;
        Row result = instance.put(c, index, kv);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of compareTo method, of class Session.
     */
    @Test
    public void testCompareTo() {
        System.out.println("compareTo");
        Session o = null;
        Session instance = new Session();
        int expResult = 0;
        int result = instance.compareTo(o);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of toString method, of class Session.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        Session instance = new Session();
        String expResult = "";
        String result = instance.toString();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of rollaback method, of class Session.
     */
    @Test
    public void testRollback() {
        System.out.println("rollaback");
        Session instance = new Session();
        instance.rollback();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of commit method, of class Session.
     */
    @Test
    public void testCommit() {
        System.out.println("commit");
        Session instance = new Session();
        instance.commit();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    public void printAll(Session mySession) {
        Row from = new Row();
        Row to = new Row();
        from.put("s_oid", Long.MIN_VALUE);
        from.put("s_timestamp", 0L);
        to.put("s_oid", Long.MAX_VALUE);
        to.put("s_timestamp", System.currentTimeMillis() * 10);
        for (Iterator<Row> it = mySession.get(testCol, from, to, "main", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            System.out.println(kp);
        }
    }
    
    public void printAll() {
        System.out.println("====== Data ======");
        Session mySession=new Session();
        Row from = new Row();
        Row to = new Row();
        from.put("s_oid", Long.MIN_VALUE);
        from.put("s_timestamp", 0L);
        to.put("s_oid", Long.MAX_VALUE);
        to.put("s_timestamp", System.currentTimeMillis() * 10);
        for (Iterator<Row> it = mySession.get(testCol, from, to, "main", false).iterator(); it.hasNext();) {
            Row kp = it.next();
            System.out.println(kp);
        }
        System.out.println("===================");
    }
}