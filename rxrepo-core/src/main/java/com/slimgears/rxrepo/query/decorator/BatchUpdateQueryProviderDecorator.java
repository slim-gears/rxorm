package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchUpdateQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final int batchSize;
    private final static Logger log = LoggerFactory.getLogger(BatchUpdateQueryProviderDecorator.class);

    protected BatchUpdateQueryProviderDecorator(QueryProvider underlyingProvider, int batchSize) {
        super(underlyingProvider);
        this.batchSize = batchSize;
    }

    public static Decorator create(int batchSize) {
        return batchSize > 0
                ? src -> new BatchUpdateQueryProviderDecorator(src, batchSize)
                : Decorator.identity();
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return Observable.fromIterable(entities)
                .buffer(batchSize)
                .flatMapCompletable(batch -> {
                    log.debug("[{}] Processing batch of {} elements", metaClass.simpleName(), batch.size());
                    return getUnderlyingProvider().insert(metaClass, batch);
                });
    }
}
