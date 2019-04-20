package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.util.generic.RecurrentThreadLocal;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

class OrientDbSessionProvider {
    private final RecurrentThreadLocal<ODatabaseDocument> databaseSessionProvider;

    private OrientDbSessionProvider(Supplier<ODatabaseDocument> databaseSessionProvider) {
        this.databaseSessionProvider = new RecurrentThreadLocal<>(databaseSessionProvider);
    }

    static OrientDbSessionProvider create(Supplier<ODatabaseDocument> dbSessionSupplier) {
        return new OrientDbSessionProvider(dbSessionSupplier);
    }

    <T> T withSession(Function<ODatabaseDocument, T> func) {
        try {
            ODatabaseDocument dbSession = databaseSessionProvider.acquire();
            dbSession.activateOnCurrentThread();
            return func.apply(dbSession);
        } finally {
            databaseSessionProvider.release();
        }
    }

    void withSession(Consumer<ODatabaseDocument> func) {
        this.<Void>withSession(session -> {
            func.accept(session);
            return null;
        });
    }
}
