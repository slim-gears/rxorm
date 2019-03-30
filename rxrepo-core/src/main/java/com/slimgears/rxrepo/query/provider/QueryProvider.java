package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface QueryProvider {
    <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity);
    <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query);
    <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query);
    <K, S extends HasMetaClassWithKey<K, S>, T, R> Single<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator);

    <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update);
    <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete);

    default <K, S extends HasMetaClassWithKey<K, S>, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        return liveQuery(query).flatMapSingle(n -> aggregate(query, aggregator));
    }

    default <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> find(
            MetaClassWithKey<K, S> metaClass,
            K id) {
        return query(QueryInfo.<K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(id))
                .build())
                .firstElement();
    }
}
