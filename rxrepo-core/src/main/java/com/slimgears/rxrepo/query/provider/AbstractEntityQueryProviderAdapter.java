package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.*;
import io.reactivex.functions.Function;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractEntityQueryProviderAdapter implements QueryProvider {
    private final Map<Class<?>, EntityQueryProvider<?, ?>> providerCache = new ConcurrentHashMap<>();

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return entities(metaClass)
                .insertOrUpdate(key, entityUpdater)
                .subscribeOn(scheduler());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return entities(query.metaClass()).query(query)
                .subscribeOn(scheduler());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return entities(query.metaClass()).liveQuery(query);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return entities(query.metaClass())
                .aggregate(query, aggregator)
                .subscribeOn(scheduler());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> update(UpdateInfo<K, S> update) {
        return entities(update.metaClass())
                .update(update)
                .subscribeOn(scheduler());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return entities(delete.metaClass())
                .delete(delete)
                .subscribeOn(scheduler());
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return entities(metaClass)
                .drop()
                .andThen(Completable.fromAction(() -> providerCache.remove(metaClass.asClass())));
    }

    @SuppressWarnings("unchecked")
    protected <K, S extends HasMetaClassWithKey<K, S>> EntityQueryProvider<K, S> entities(MetaClassWithKey<K, S> metaClass) {
        return (EntityQueryProvider<K, S>) providerCache.computeIfAbsent(metaClass.asClass(), c -> createProvider(metaClass));
    }

    @Override
    public Completable dropAll() {
        return dropAllProviders()
                .andThen(Completable.fromAction(providerCache::clear));
    }

    protected abstract Completable dropAllProviders();
    protected abstract <K, S extends HasMetaClassWithKey<K, S>> EntityQueryProvider<K, S> createProvider(MetaClassWithKey<K, S> metaClass);
    protected abstract Scheduler scheduler();
}
