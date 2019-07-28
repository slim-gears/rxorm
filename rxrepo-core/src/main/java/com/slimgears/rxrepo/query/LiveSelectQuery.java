package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;
import io.reactivex.functions.IntFunction;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class LiveSelectQuery<K, S extends HasMetaClassWithKey<K, S>, T> {
    public abstract Observable<T> first();
    public abstract Observable<List<T>> toList();
    public abstract LiveSelectQuery<K, S, T> properties(Iterable<PropertyExpression<T, ?, ?>> properties);
    public abstract <R> Observable<R> aggregate(Aggregator<T, T, R> aggregator);
    public abstract <R> Observable<R> observeAs(QueryTransformer<K, S, T, R> transformer);
    public abstract Observable<Notification<T>> queryAndObserve();
    public abstract Observable<Notification<T>> observe();

    public Observable<Long> count() {
        return aggregate(Aggregator.count());
    }

    public Observable<T[]> toArray(IntFunction<T[]> arrayCreator) {
        return toList().map(list -> list.toArray(arrayCreator.apply(list.size())));
    }

    public <R> R apply(Function<LiveSelectQuery<K, S, T>, R> mapper) {
        return mapper.apply(this);
    }


    @SafeVarargs
    public final Observable<Notification<T>> queryAndObserve(PropertyExpression<T, ?, ?>... properties) {
        return properties(properties).queryAndObserve();
    }

    @SafeVarargs
    public final Observable<Notification<T>> observe(PropertyExpression<T, ?, ?>... properties) {
        return properties(properties).observe();
    }

    @SafeVarargs
    public final LiveSelectQuery<K, S, T> properties(PropertyExpression<T, ?, ?>... properties) {
        return properties(Arrays.asList(properties));
    }
}
