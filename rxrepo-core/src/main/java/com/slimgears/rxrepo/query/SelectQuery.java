package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

public abstract class SelectQuery<T> {
    public abstract Maybe<T> first();
    public abstract <R, E extends UnaryOperationExpression<T, Collection<T>, R>> Single<R> aggregate(Aggregator<T, T, R, E> aggregator);

    @SafeVarargs
    public final Observable<T> retrieve(PropertyExpression<T, ?, ?>... properties) {
        return retrieve(Arrays.asList(properties));
    }

    public <R> R apply(Function<SelectQuery<T>, R> mapper) {
        return mapper.apply(this);
    }

    public Observable<T> retrieve() {
        //noinspection unchecked
        return retrieve(new PropertyExpression[0]);
    }

    public Single<Long> count() {
        return aggregate(Aggregator.count());
    }

    protected abstract Observable<T> retrieve(Collection<PropertyExpression<T, ?, ?>> properties);
}
