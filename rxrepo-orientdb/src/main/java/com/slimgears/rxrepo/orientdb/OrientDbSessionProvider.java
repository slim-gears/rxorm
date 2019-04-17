package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.util.generic.RecurrentThreadLocal;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class OrientDbSessionProvider {
    private final RecurrentThreadLocal<ODatabaseDocument> databaseSessionProvider;

    public OrientDbSessionProvider(Supplier<ODatabaseDocument> databaseSessionProvider) {
        this.databaseSessionProvider = new RecurrentThreadLocal<>(databaseSessionProvider);
    }

    <T> T withSession(Function<ODatabaseDocument, T> func) {
        try {
            return func.apply(databaseSessionProvider.acquire());
        } finally {
            databaseSessionProvider.release();
        }
    }

    void withSession(Consumer<ODatabaseDocument> func) {
        try {
            func.accept(databaseSessionProvider.acquire());
        } finally {
            databaseSessionProvider.release();
        }
    }
}
