package com.slimgears.rxrepo.expressions;

import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanConstantExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanUnaryOperationExpression;

import java.util.Arrays;

public interface BooleanExpression<S> extends ObjectExpression<S, Boolean> {
    default BooleanExpression<S> and(ObjectExpression<S, Boolean> value) {
        return BooleanBinaryOperationExpression.create(Type.And, this, value);
    }

    default BooleanExpression<S> or(ObjectExpression<S, Boolean> value) {
        return BooleanBinaryOperationExpression.create(Type.Or, this, value);
    }

    default BooleanExpression<S> not() {
        return BooleanUnaryOperationExpression.create(Type.Not, this);
    }

    static <S> BooleanExpression<S> ofTrue() {
        return BooleanConstantExpression.create(Type.BooleanConstant, true);
    }

    static <S> BooleanExpression<S> ofFalse() {
        return BooleanConstantExpression.create(Type.BooleanConstant, false);
    }

    static <S> BooleanExpression<S> not(ObjectExpression<S, Boolean> source) {
        return BooleanUnaryOperationExpression.create(Type.Not, source);
    }

    @SafeVarargs
    static <S> BooleanExpression<S> and(ObjectExpression<S, Boolean> first, ObjectExpression<S, Boolean>... expressions) {
        return ObjectExpression.asBoolean(
                Arrays.stream(expressions)
                        .reduce(first, (a, b) -> BooleanBinaryOperationExpression.create(Type.And, a, b)));
    }

    @SafeVarargs
    static <S> BooleanExpression<S> or(ObjectExpression<S, Boolean> first, ObjectExpression<S, Boolean>... expressions) {
        return ObjectExpression.asBoolean(
                Arrays.stream(expressions)
                        .reduce(first, (a, b) -> BooleanBinaryOperationExpression.create(Type.Or, a, b)));
    }
}
