package com.slimgears.rxrepo.filters;

import com.slimgears.rxrepo.annotations.FilterPrototype;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ComparableExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;
import java.util.Optional;

@FilterPrototype
public interface ComparableFilterPrototype<T extends Comparable<T>> extends ValueFilterPrototype<T> {
    @Nullable T lessThan();
    @Nullable T greaterThan();
    @Nullable T lessOrEqual();
    @Nullable T greaterOrEqual();

    @Override
    default <S> Optional<BooleanExpression<S>> toExpression(ObjectExpression<S, T> arg) {
        ComparableExpression<S, T> comparableArg = ObjectExpression.asComparable(arg);
        return Filters.combine(
                ValueFilterPrototype.super.toExpression(comparableArg),
                Optional.ofNullable(lessThan()).map(comparableArg::lessThan),
                Optional.ofNullable(lessOrEqual()).map(comparableArg::lessOrEqual),
                Optional.ofNullable(greaterThan()).map(comparableArg::greaterThan),
                Optional.ofNullable(greaterOrEqual()).map(comparableArg::greaterOrEqual));
    }

}
