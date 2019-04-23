package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.concurrent.TimeUnit;

public interface QueryProvider {
    <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity);
    <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater);
    <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query);
    <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query);
    <K, S extends HasMetaClassWithKey<K, S>, T, R> Single<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator);

    <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update);
    <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete);

    default <K, S extends HasMetaClassWithKey<K, S>, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        return liveQuery(query).debounce(500, TimeUnit.MILLISECONDS).switchMapSingle(n -> aggregate(query, aggregator));
    }
}
