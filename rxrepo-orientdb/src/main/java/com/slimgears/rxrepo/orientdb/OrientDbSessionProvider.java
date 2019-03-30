package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.util.function.Supplier;

public class OrientDbSessionProvider implements Supplier<ODatabaseDocument> {
    private final ThreadLocal<ODatabaseDocument> databaseSessionThreadLocal;

    interface Closeable extends java.io.Closeable {
        void close();
    }

    public OrientDbSessionProvider(Supplier<ODatabaseDocument> databaseSessionProvider) {
        this.databaseSessionThreadLocal = ThreadLocal.withInitial(databaseSessionProvider);
    }

    @Override
    public ODatabaseDocument get() {
        ODatabaseDocument session = databaseSessionThreadLocal.get();
        session.activateOnCurrentThread();
        return session;
    }
}
