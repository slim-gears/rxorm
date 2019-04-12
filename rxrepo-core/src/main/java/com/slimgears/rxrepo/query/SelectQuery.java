package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Collection;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public interface SelectQuery<T> {
    Maybe<T> first();
    <R, E extends UnaryOperationExpression<T, Collection<T>, R>> Single<R> aggregate(Aggregator<T, T, R, E> aggregator);
    Observable<T> retrieve(PropertyExpression<T, ?, ?>... properties);

    default <R> R apply(Function<SelectQuery<T>, R> mapper) {
        return mapper.apply(this);
    }

    default Observable<T> retrieve() {
        //noinspection unchecked
        return retrieve(new PropertyExpression[0]);
    }

    default Single<Long> count() {
        return aggregate(Aggregator.count());
    }
}
