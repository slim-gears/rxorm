package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import io.reactivex.Observable;
import io.reactivex.functions.IntFunction;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public interface LiveSelectQuery<T> {
    default Observable<Long> count() {
        return aggregate(Aggregator.count());
    }

    default Observable<T[]> toArray(IntFunction<T[]> arrayCreator) {
        return toList().map(list -> list.toArray(arrayCreator.apply(list.size())));
    }

    Observable<T> first();
    Observable<List<? extends T>> toList();
    <R, E extends UnaryOperationExpression<T, Collection<T>, R>> Observable<R> aggregate(Aggregator<T, T, R, E> aggregator);
    Observable<Notification<T>> observe(PropertyExpression<T, ?, ?>... properties);

    default <R> R apply(Function<LiveSelectQuery<T>, R> mapper) {
        return mapper.apply(this);
    }

    default Observable<Notification<T>> observe() {
        //noinspection unchecked
        return observe(new PropertyExpression[0]);
    }
}
