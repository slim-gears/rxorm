package com.slimgears.rxrepo.expressions;

import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;

public interface ComparableExpression<S, T extends Comparable<T>> extends ObjectExpression<S, T> {
    default BooleanExpression<S> lessThan(T value) {
        return lessThan(ConstantExpression.of(value));
    }

    default BooleanExpression<S> lessThan(ObjectExpression<S, T> value) {
        return BooleanBinaryOperationExpression.create(Type.LessThan, this, value);
    }

    default BooleanExpression<S> greaterThan(T value) {
        return greaterThan(ConstantExpression.of(value));
    }

    default BooleanExpression<S> greaterThan(ObjectExpression<S, T> value) {
        return BooleanBinaryOperationExpression.create(Type.GreaterThan, this, value);
    }

    default BooleanExpression<S> lessOrEqual(ObjectExpression<S, T> value) {
        return greaterThan(value).not();
    }

    default BooleanExpression<S> lessOrEqual(T value) {
        return greaterThan(value).not();
    }

    default BooleanExpression<S> greaterOrEqual(ObjectExpression<S, T> value) {
        return lessThan(value).not();
    }

    default BooleanExpression<S> greaterOrEqual(T value) {
        return lessThan(value).not();
    }

    default BooleanExpression<S> betweenInclusive(ObjectExpression<S, T> min, ObjectExpression<S, T> max) {
        return lessThan(min).or(greaterThan(max)).not();
    }

    default BooleanExpression<S> betweenExclusive(ObjectExpression<S, T> min, ObjectExpression<S, T> max) {
        return greaterThan(min).and(lessThan(max));
    }

    default BooleanExpression<S> betweenInclusive(T min, T max) {
        return lessThan(min).or(greaterThan(max)).not();
    }

    default BooleanExpression<S> betweenExclusive(T min, T max) {
        return greaterThan(min).and(lessThan(max));
    }
}
