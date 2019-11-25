package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.filters.Filter;

import java.util.function.Consumer;
import java.util.function.Function;

public interface QueryBuilder<__B extends QueryBuilder<__B, S>, S> {
    __B where(ObjectExpression<S, Boolean> predicate);
    __B limit(long limit);
    __B where(Filter<S> filter);

    @SuppressWarnings("unchecked")
    default __B apply(Function<__B, __B> config) {
        return config.apply((__B)this);
    }

    @SuppressWarnings("unchecked")
    default __B apply(Consumer<__B> config) {
        config.accept((__B)this);
        return (__B)this;
    }
}
