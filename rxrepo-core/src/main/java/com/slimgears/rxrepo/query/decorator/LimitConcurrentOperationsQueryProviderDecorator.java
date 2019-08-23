package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Collections;
import java.util.concurrent.Semaphore;

public class LimitConcurrentOperationsQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Semaphore availableOperations;

    private LimitConcurrentOperationsQueryProviderDecorator(QueryProvider underlyingProvider, int maxOperations) {
        super(underlyingProvider);
        this.availableOperations = new Semaphore(maxOperations);
    }

    public static QueryProvider.Decorator create(int maxConcurrentOperations) {
        return qp -> new LimitConcurrentOperationsQueryProviderDecorator(qp, maxConcurrentOperations);
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return Observable.fromIterable(entities)
                .flatMapCompletable(e -> super.insert(metaClass, Collections.singleton(e))
                        .doOnSubscribe(d -> doOnSubscribe())
                        .doFinally(this::doFinally));
    }

    @Override
    public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
        return super.insertOrUpdate(metaClass, entity)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, entityUpdater)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return super.query(query)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.aggregate(query, aggregator)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return super.update(update)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return super.delete(delete)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return super.drop(metaClass)
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    @Override
    public Completable dropAll() {
        return super.dropAll()
                .doOnSubscribe(d -> doOnSubscribe())
                .doFinally(this::doFinally);
    }

    private void doOnSubscribe() {
        try {
            availableOperations.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void doFinally() {
        availableOperations.release();
    }
}
