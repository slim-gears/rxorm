package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheQueryProvider implements QueryProvider {
    private final QueryProvider underlyingProvider;
    private final Map<QueryInfo, Observable<Notification<?>>> activeQueries = new ConcurrentHashMap<>();

    private CacheQueryProvider(QueryProvider underlyingProvider) {
        this.underlyingProvider = underlyingProvider;
    }

    public static QueryProvider of(QueryProvider queryProvider) {
        return new CacheQueryProvider(queryProvider);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity) {
        return underlyingProvider.insertOrUpdate(entity);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return underlyingProvider.insertOrUpdate(metaClass, key, entityUpdater);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return underlyingProvider.query(query);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return (Observable<Notification<T>>)(Observable)activeQueries
                .computeIfAbsent(query, q -> (Observable<Notification<?>>)(Observable)underlyingProvider
                        .liveQuery(query)
                        .doOnTerminate(() -> activeQueries.remove(query))
                        .share());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Single<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        return underlyingProvider.aggregate(query, aggregator);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update) {
        return underlyingProvider.update(update);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return underlyingProvider.delete(delete);
    }
}
