package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.slimgears.util.generic.LazyString.lazy;

public class AbstractQueryProviderDecorator implements QueryProvider {
    private final QueryProvider underlyingProvider;
    protected final Logger log;

    protected AbstractQueryProviderDecorator(QueryProvider underlyingProvider) {
        this.log = LoggerFactory.getLogger(getClass());
        this.underlyingProvider = underlyingProvider;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return underlyingProvider.insert(metaClass, entities);
    }

    @Override
    public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
        return underlyingProvider.insertOrUpdate(metaClass, entity);
    }

    @Override
    public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return underlyingProvider.insertOrUpdate(metaClass, key, entityUpdater);
    }

    @Override
    public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return underlyingProvider.query(query);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> query) {
        return underlyingProvider.queryAndObserve(query);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return underlyingProvider.queryAndObserve(queryInfo, observeInfo);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return underlyingProvider.liveQuery(query)
            .doOnNext(n -> log.trace("[{}] Received notification: {}", lazy(() -> getClass().getSimpleName()), n));
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return underlyingProvider.aggregate(query, aggregator);
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return underlyingProvider.liveAggregate(query, aggregator)
            .doOnNext(n -> log.trace("[{}] Received aggregation notification: {}", lazy(() -> getClass().getSimpleName()), n));
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return underlyingProvider.update(update);
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return underlyingProvider.delete(delete);
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return underlyingProvider.drop(metaClass);
    }

    @Override
    public Completable dropAll() {
        return underlyingProvider.dropAll();
    }

    @Override
    public void close() {
        underlyingProvider.close();
    }
}
