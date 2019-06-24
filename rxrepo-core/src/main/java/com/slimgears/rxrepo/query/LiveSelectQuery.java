package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import io.reactivex.Observable;
import io.reactivex.functions.IntFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings("WeakerAccess")
public abstract class LiveSelectQuery<T> {
    public abstract Observable<T> first();
    public abstract Observable<List<T>> toList();
    public abstract <R, E extends UnaryOperationExpression<T, Collection<T>, R>> Observable<R> aggregate(Aggregator<T, T, R, E> aggregator);

    public Observable<Long> count() {
        return aggregate(Aggregator.count());
    }

    public Observable<T[]> toArray(IntFunction<T[]> arrayCreator) {
        return toList().map(list -> list.toArray(arrayCreator.apply(list.size())));
    }

    public <R> R apply(Function<LiveSelectQuery<T>, R> mapper) {
        return mapper.apply(this);
    }

    @SafeVarargs
    public final Observable<Notification<T>> queryAndObserve(PropertyExpression<T, ?, ?>... properties) {
        return queryAndObserve(Arrays.asList(properties));
    }

    @SafeVarargs
    public final Observable<Notification<T>> observe(PropertyExpression<T, ?, ?>... properties) {
        return observe(Arrays.asList(properties));
    }

    public Observable<Notification<T>> queryAndObserve() {
        return queryAndObserve(Collections.emptyList());
    }

    public Observable<Notification<T>> observe() {
        return observe(Collections.emptyList());
    }

    protected abstract Observable<Notification<T>> observe(Collection<PropertyExpression<T, ?, ?>> properties);
    protected abstract Observable<Notification<T>> queryAndObserve(Collection<PropertyExpression<T, ?, ?>> properties);
}
