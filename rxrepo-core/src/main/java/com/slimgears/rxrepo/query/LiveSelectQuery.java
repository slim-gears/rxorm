package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import io.reactivex.Observable;
import io.reactivex.functions.IntFunction;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class LiveSelectQuery<T> {
    public abstract Observable<T> first();
    public abstract LiveSelectQuery<T> properties(Iterable<PropertyExpression<T, ?, ?>> properties);
    public abstract <R> Observable<R> aggregate(Aggregator<T, T, R> aggregator);
    public abstract <R> Observable<R> observeAs(QueryTransformer<T, R> transformer);
    public abstract Observable<Notification<T>> queryAndObserve();
    public abstract Observable<Notification<T>> observe();

    public Observable<Long> count() {
        return aggregate(Aggregator.count());
    }

    public Observable<T[]> toArray(IntFunction<T[]> arrayCreator) {
        return asList().map(list -> list.toArray(arrayCreator.apply(list.size())));
    }

    public <R> R apply(Function<LiveSelectQuery<T>, R> mapper) {
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
    public final LiveSelectQuery<T> properties(PropertyExpression<T, ?, ?>... properties) {
        return properties(Arrays.asList(properties));
    }

    public final Observable<List<T>> asList() {
        return observeAs(Notifications.toList());
    }

    @SafeVarargs
    public final Observable<List<T>> asList(PropertyExpression<T, ?, ?>... properties) {
        return observeAs(Notifications.toList(), properties);
    }

    @SafeVarargs
    public final <R> Observable<R> observeAs(QueryTransformer<T, R> transformer, PropertyExpression<T, ?, ?>... properties) {
        return properties(properties).observeAs(transformer);
    }
}
