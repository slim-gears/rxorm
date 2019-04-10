package com.slimgears.rxrepo.filters;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.annotations.FilterPrototype;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;
import java.util.Optional;

@FilterPrototype
public interface ValueFilterPrototype<T> extends Filter<T> {
    @Nullable T equalsTo();
    @Nullable ImmutableList<T> equalsToAny();

    @Override
    default <S> Optional<BooleanExpression<S>> toExpression(ObjectExpression<S, T> arg) {
        return Filters.combineExpressions(
                Optional.ofNullable(equalsTo()).map(arg::eq),
                Optional.ofNullable(equalsToAny()).map(arg::in));
    }
}
