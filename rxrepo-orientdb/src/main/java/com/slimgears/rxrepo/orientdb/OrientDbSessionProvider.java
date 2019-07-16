package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.util.generic.RecurrentThreadLocal;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

class OrientDbSessionProvider {
    private final RecurrentThreadLocal<ODatabaseDocument> databaseSessionProvider;

    private OrientDbSessionProvider(Supplier<ODatabaseDocument> databaseSessionProvider,
                                    Consumer<ODatabaseDocument> onRelease) {
        this.databaseSessionProvider = RecurrentThreadLocal
                .of(databaseSessionProvider)
                .onRelease(onRelease);
    }

    static OrientDbSessionProvider create(Supplier<ODatabaseDocument> dbSessionSupplier) {
        return new OrientDbSessionProvider(dbSessionSupplier, ODatabaseDocument::close);
    }

    static OrientDbSessionProvider create(Supplier<ODatabaseDocument> dbSessionSupplier, Consumer<ODatabaseDocument> onClose) {
        return new OrientDbSessionProvider(dbSessionSupplier, session -> {
            onClose.accept(session);
            session.close();
        });
    }

    static OrientDbSessionProvider create(ODatabaseDocument dbSessionSupplier) {
        return new OrientDbSessionProvider(() -> dbSessionSupplier, db -> {});
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
