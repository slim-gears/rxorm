package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.expressions.PropertyExpression;

import java.util.function.Function;

public interface PropertyExpressionValueProvider<S, T, V> {
    PropertyExpression<S, T, V> property();
    V value(S object);

    static <S, T, V> PropertyExpressionValueProvider<S, T, V> fromProperty(PropertyExpression<S, T, V> propertyExpression) {
        return DefaultExpressionValueProvider.fromProperty(propertyExpression);
    }
}
