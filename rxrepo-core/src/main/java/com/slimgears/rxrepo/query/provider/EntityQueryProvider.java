package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public interface EntityQueryProvider<K, S> extends AutoCloseable {
    MetaClassWithKey<K, S> metaClass();
    Maybe<Supplier<S>> insertOrUpdate(K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater);
    <T> Observable<T> query(QueryInfo<K, S, T> query);
    <T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query);
    <T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator);
    Single<Integer> update(UpdateInfo<K, S> update);
    Single<Integer> delete(DeleteInfo<K, S> delete);
    Completable drop();

    default Completable insert(Iterable<S> entities, boolean recursive) {
        return Observable.fromIterable(entities)
                .concatMapEager(e -> insertOrUpdate(e, recursive).toObservable())
                .ignoreElements();
    }

    default Single<Supplier<S>> insertOrUpdate(S entity, boolean recursive) {
        K key = metaClass().keyOf(entity);
        return insertOrUpdate(key, recursive, val -> val
                .map(e -> MetaClasses.merge(metaClass(), e, entity))
                .switchIfEmpty(Maybe.just(entity)))
                .toSingle();
    }

    default <T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query).debounce(500, TimeUnit.MILLISECONDS).switchMapMaybe(n -> aggregate(query, aggregator));
    }
}
