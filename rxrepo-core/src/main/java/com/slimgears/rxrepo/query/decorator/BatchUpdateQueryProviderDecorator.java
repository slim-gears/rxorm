package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Observable;

public class BatchUpdateQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final int batchSize;

    protected BatchUpdateQueryProviderDecorator(QueryProvider underlyingProvider, int batchSize) {
        super(underlyingProvider);
        this.batchSize = batchSize;
    }

    public static QueryProvider.Decorator create(int batchSize) {
        return batchSize > 0
                ? src -> new BatchUpdateQueryProviderDecorator(src, batchSize)
                : QueryProvider.Decorator.identity();
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return Observable.fromIterable(entities)
                .buffer(batchSize)
                .flatMapCompletable(batch -> getUnderlyingProvider()
                        .insert(metaClass, batch, recursive));
    }
}
