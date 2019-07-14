package com.slimgears.rxrepo.filters;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.annotations.FilterPrototype;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;
import java.util.Optional;

@FilterPrototype
public interface ValueFilterPrototype<T> extends ObjectFilterPrototype<T> {
    @Nullable T equalsTo();
    @Nullable ImmutableList<T> equalsToAny();
    @Nullable T notEqualsTo();

    @Override
    default <S> Optional<BooleanExpression<S>> toExpression(ObjectExpression<S, T> arg) {
        return Filters.combineExpressions(
                ObjectFilterPrototype.super.toExpression(arg),
                Optional.ofNullable(equalsTo()).map(arg::eq),
                Optional.ofNullable(equalsToAny()).map(arg::in),
                Optional.ofNullable(notEqualsTo()).map(arg::notEq));
    }
}
