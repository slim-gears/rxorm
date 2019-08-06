package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.concurrent.TimeUnit;

public interface EntityQueryProvider<K, S extends HasMetaClassWithKey<K, S>> {
    Maybe<S> insertOrUpdate(K key, Function<Maybe<S>, Maybe<S>> entityUpdater);
    <T> Observable<T> query(QueryInfo<K, S, T> query);
    <T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query);
    <T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator);
    Single<Integer> update(UpdateInfo<K, S> update);
    Single<Integer> delete(DeleteInfo<K, S> delete);
    Completable drop();

    default Single<S> insertOrUpdate(S entity) {
        K key = HasMetaClassWithKey.keyOf(entity);
        return insertOrUpdate(key, val -> val
                .map(e -> e.merge(entity))
                .switchIfEmpty(Maybe.just(entity)))
                .toSingle();
    }

    default <T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query).debounce(500, TimeUnit.MILLISECONDS).switchMapMaybe(n -> aggregate(query, aggregator));
    }
}
