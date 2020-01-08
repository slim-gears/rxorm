package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.Orient;
import com.slimgears.rxrepo.query.decorator.AbstractQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("WeakerAccess")
class OrientDbShutdownQueryProviderDecorator extends AbstractQueryProviderDecorator {

    private static AtomicInteger instances = new AtomicInteger();

    private OrientDbShutdownQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);

        if (instances.incrementAndGet() == 1) {
            Orient.instance().startup();
        }
    }

    public static Decorator create() {
        return OrientDbShutdownQueryProviderDecorator::new;
    }

    @Override
    public void close() {
        if (instances.decrementAndGet() == 0) {
            Orient.instance().shutdown();
        }
    }
}
