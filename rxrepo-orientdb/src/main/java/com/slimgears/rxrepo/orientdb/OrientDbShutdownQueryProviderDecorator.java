package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.slimgears.rxrepo.query.decorator.AbstractQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;

import java.util.function.Supplier;

@SuppressWarnings("WeakerAccess")
class OrientDbShutdownQueryProviderDecorator extends AbstractQueryProviderDecorator {

    private OrientDbShutdownQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static Decorator create() {
        return OrientDbShutdownQueryProviderDecorator::new;
    }

    @Override
    public void close() {
        Orient.instance().shutdown();
    }
}
