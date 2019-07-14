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
    public abstract <R, E extends UnaryOperationExpression<T, Collection<T>, R>> Maybe<R> aggregate(Aggregator<T, T, R, E> aggregator);
    public abstract SelectQuery<T> properties(Iterable<PropertyExpression<T, ?, ?>> properties);

    @SafeVarargs
    public final Observable<T> retrieve(PropertyExpression<T, ?, ?>... properties) {
        return properties(properties).retrieve();
    }

    @SafeVarargs
    public final SelectQuery<T> properties(PropertyExpression<T, ?, ?>... properties) {
        return properties(Arrays.asList(properties));
    }

    public <R> R apply(Function<SelectQuery<T>, R> mapper) {
        return mapper.apply(this);
    }

    public abstract Observable<T> retrieve();

    public Single<Long> count() {
        return aggregate(Aggregator.count()).toSingle(0L);
    }
}
