package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.filters.Filter;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Function;

public interface QueryBuilder<__B extends QueryBuilder<__B, K, S>, K, S extends HasMetaClassWithKey<K, S>> {
    __B where(BooleanExpression<S> predicate);
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
