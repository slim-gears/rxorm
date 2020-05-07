package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.Timeout;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.time.Duration;
import java.util.function.Supplier;

public class OperationTimeoutQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Duration queryTimeout;
    private final Duration updateTimeout;

    private OperationTimeoutQueryProviderDecorator(QueryProvider underlyingProvider, Duration queryTimeout, Duration updateTimeout) {
        super(underlyingProvider);
        this.queryTimeout = queryTimeout;
        this.updateTimeout = updateTimeout;
    }

    public static QueryProvider.Decorator create(Duration queryTimeout, Duration updateTimeout) {
        return src -> new OperationTimeoutQueryProviderDecorator(src, queryTimeout, updateTimeout);
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, recursive)
                .compose(Timeout.forCompletable(updateTimeout));
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return super.insertOrUpdate(metaClass, entity, recursive)
                .compose(Timeout.forSingle(updateTimeout));
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, recursive, entityUpdater)
                .compose(Timeout.forMaybe(updateTimeout));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
        return super.query(query)
                .compose(Timeout.forObservable(queryTimeout));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(queryInfo, observeInfo)
                .compose(Timeout.tillFirst(queryTimeout));
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.aggregate(query, aggregator)
                .compose(Timeout.forMaybe(queryTimeout));
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.liveAggregate(query, aggregator)
                .compose(Timeout.tillFirst(queryTimeout));
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return super.update(update)
                .compose(Timeout.forSingle(updateTimeout));
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return super.delete(delete)
                .compose(Timeout.forSingle(updateTimeout));
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return super.drop(metaClass)
                .compose(Timeout.forCompletable(updateTimeout));
    }

    @Override
    public Completable dropAll() {
        return super.dropAll();
    }
}
