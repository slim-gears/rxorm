package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

public interface QueryProvider extends AutoCloseable {
    <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity);
    <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater);
    <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query);
    <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query);
    <K, S extends HasMetaClassWithKey<K, S>, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator);

    <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update);
    <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete);
    Completable drop();

    default <K, S extends HasMetaClassWithKey<K, S>, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        return liveQuery(query).debounce(500, TimeUnit.MILLISECONDS).switchMapMaybe(n -> aggregate(query, aggregator));
    }

    default void close() {
    }

    @FunctionalInterface
    interface Decorator extends UnaryOperator<QueryProvider> {
        default Decorator andThen(Decorator decorator) {
            return qp -> decorator.apply(this.apply(qp));
        }

        static Decorator identity() {
            return qp -> qp;
        }

        static Decorator of(Decorator... decorators) {
            return Arrays.stream(decorators)
                    .reduce((first, second) -> first.andThen(second))
                    .orElseGet(Decorator::identity);
        }
    }
}
