package com.slimgears.util.repository.query;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.repository.expressions.Aggregator;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.List;

public interface QueryProvider {
    <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> find(
            MetaClassWithKey<K, S> metaClass,
            K id);

    <K, S extends HasMetaClassWithKey<K, S>> Single<List<S>> update(
            MetaClassWithKey<K, S> meta,
            Iterable<S> entities);

    <K, S extends HasMetaClassWithKey<K, S>> Single<List<S>> insert(
            MetaClassWithKey<K, S> meta,
            Iterable<S> entities);

    <K, S extends HasMetaClassWithKey<K, S>, T, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasSortingInfo<S>
            & HasProperties<T>> Observable<T> query(Q query);

    <K, S extends HasMetaClassWithKey<K, S>, T, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasSortingInfo<S>
            & HasProperties<T>> Observable<Notification<T>> liveQuery(Q query);

    <K, S extends HasMetaClassWithKey<K, S>, T, R, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasProperties<T>> Single<R> aggregate(Q query, Aggregator<T, T, R, ?> aggregator);

    <K, S extends HasMetaClassWithKey<K, S>, T, R, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasProperties<T>> Observable<R> liveAggregate(Q query, Aggregator<T, T, R, ?> aggregator);

    <K, S extends HasMetaClassWithKey<K, S>, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasPropertyUpdates<S>> Observable<S> update(Q update);

    <K, S extends HasMetaClassWithKey<K, S>, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>> Completable delete(Q delete);
}
