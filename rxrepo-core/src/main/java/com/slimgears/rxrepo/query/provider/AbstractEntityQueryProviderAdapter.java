package com.slimgears.rxrepo.query.provider;

import com.google.common.collect.Iterables;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractEntityQueryProviderAdapter implements QueryProvider {
    private final Map<Class<?>, EntityQueryProvider<?, ?>> providerCache = new ConcurrentHashMap<>();

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Completable insert(Iterable<S> entities) {
        return Optional.ofNullable(Iterables.getFirst(entities, null))
                .map(HasMetaClassWithKey::metaClass)
                .map(this::entities)
                .map(e -> e.insert(entities))
                .orElseGet(Completable::complete);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return entities(metaClass)
                .insertOrUpdate(key, entityUpdater);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return entities(query.metaClass()).query(query);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return entities(query.metaClass()).liveQuery(query);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return entities(query.metaClass())
                .aggregate(query, aggregator);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> update(UpdateInfo<K, S> update) {
        return entities(update.metaClass())
                .update(update);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return entities(delete.metaClass())
                .delete(delete);
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

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return entities(query.metaClass()).liveAggregate(query, aggregator);
    }

    protected abstract Completable dropAllProviders();
    protected abstract <K, S extends HasMetaClassWithKey<K, S>> EntityQueryProvider<K, S> createProvider(MetaClassWithKey<K, S> metaClass);
}
