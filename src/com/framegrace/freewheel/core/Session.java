package com.framegrace.freewheel.core;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 *
 * @author marc
 */
public class Session implements Comparable<Session> {

    Long s_id;
    ConcurrentSkipListSet<Collection> collections = new ConcurrentSkipListSet<>();
    Long MAX = Long.MAX_VALUE;
    Long MIN = Long.MIN_VALUE;
    boolean commited = false;
    long sessionStart;
    Session parent;

    private static class CurrentSessionPredicate implements Predicate<Row> {

        private final Session session;

        private CurrentSessionPredicate(Session s) {
            this.session = s;
        }

        @Override
        public boolean apply(final Row s) {
            Session s2 = (Session) s.get("s_sid");
            Boolean deleted = (Boolean) s.get("s_deleted");
            return (s2 == null || s2.equals(session)) && (deleted == null || !deleted);
        }
    }
    CurrentSessionPredicate sessionFilter;

    public Session() {
        this(null);
        sessionFilter = new CurrentSessionPredicate(this);
    }

    public Session(Session parent) {
        this.parent = parent;
        s_id = utils.getUUID();
        sessionStart = System.currentTimeMillis();
    }

    public Session subSession() {
        return new Session(this);
    }

    public Row get(Collection c, long oid) {
        Row from = new Row();
        Row to = new Row();
        from.put("s_oid", oid);
        from.put("s_timestamp", MIN);
        to.put("s_oid", oid);
        to.put("s_timestamp", System.currentTimeMillis() * 10);
        return get(c, from, to, "main", true).iterator().next();
    }

    public Iterable<Row> get(Collection c, Row from, Row to, String index, boolean descending) {
        Index i = c.indexes.get(index);
        if (!i.unique) {
            from.put("s_oid", Long.MIN_VALUE);
            to.put("s_oid", Long.MAX_VALUE);
        }
        ConcurrentNavigableMap<Row, Row> result = i.subMap(from, true, to, true);
        return Collections2.filter(descending ? result.descendingKeySet() : result.keySet(), sessionFilter);
    }

    public void insert(Collection c, Row kv) throws FWException {
        collections.add(c);
        Session subSession = new Session(this);
        kv.put("s_sid", subSession);
        for (Index i : c.indexes.values()) {
            subSession.put(c, i, kv);
        }
        subSession.commit();
    }

    public void delete(Collection c, Row kv, Index i) {
        collections.add(c);
        Row rkv = i.get(kv);
        if (rkv != null) {
            rkv.put("s_deleted", true);
        }
    }

    // Normal put, replaces. We also add commodity parameters
    public Row put(Collection c, Index index, Row kv) throws FWException {
        collections.add(c);
        Row rkv = index.get(kv);
        if (rkv == null) {
            index.put(kv, kv);
        }
        return rkv;
    }

    @Override
    public int compareTo(Session o) {
        return s_id.compareTo(o.s_id);
    }

    @Override
    public String toString() {
        return s_id == null ? "" : s_id.toString();
    }

    public void rollback() {
        Row from = new Row();
        Row to = new Row();
        from.put("s_sid", this);
        from.put("s_oid", MIN);
        to.put("s_sid", this);
        to.put("s_oid", MAX);
        ArrayList<Row> dereference_list = new ArrayList<>();
        for (Collection c : collections) {
            for (Row tod : c.session_index.subMap(from, true, to, true).keySet()) {
                if (parent == null) {
                    c.session_index.remove(tod);
                }
                Boolean deleted = (Boolean) tod.remove("s_deleted");
                if (deleted == null) {
                    deleted = false;
                }
                if (deleted) {
                    tod.remove("s_deleted");
                    dereference_list.add(tod);
                } else {
                    for (Index i : c.indexes.values()) {
                        i.remove(tod);
                    }
                }
            }
        }
        // Atomic commit. QUeries shoud behave the same with 0 or non-present
        // (s_id must not be part of any other index appart of the above temporal
        // internal session index.
        s_id = 0L;
        // Remove references (Could be done in another thread)
        // Allows nested sessions.
        for (Row d : dereference_list) {
            if (parent == null) {
                d.remove("s_sid");
            } else {
                d.put("s_sid", parent);
            }
        }
        commited = true;
    }

    public void commit() {
        Row from = new Row();
        Row to = new Row();
        from.put("s_sid", this);
        from.put("s_oid", MIN);
        to.put("s_sid", this);
        to.put("s_oid", MAX);
        ArrayList<Row> dereference_list = new ArrayList<>();
        for (Collection c : collections) {
            for (Row tod : c.session_index.subMap(from, true, to, true).keySet()) {
                if (parent == null) {
                    c.session_index.remove(tod);
                }
                dereference_list.add(tod);
            }
        }
        // Atomic commit. QUeries shoud behave the same with 0 or non-present
        // (s_id must not be part of any other index appart of the above temporal
        // internal session index.
        s_id = 0L;
        // Remove references (Could be done in another thread)
        // Allows nested sessions.
        for (Row d : dereference_list) {
            if (parent == null) {
                d.remove("s_sid");
            } else {
                d.put("s_sid", parent);
            }
        }
        commited = true;
    }
}
