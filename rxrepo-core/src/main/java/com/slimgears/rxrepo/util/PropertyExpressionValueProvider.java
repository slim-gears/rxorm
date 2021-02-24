package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.expressions.PropertyExpression;

import java.util.function.Function;

public interface PropertyExpressionValueProvider<S, T, V> {
    PropertyExpression<S, T, V> property();
    V value(S object);

    static <S, T, V> PropertyExpressionValueProvider<S, T, V> fromProperty(PropertyExpression<S, T, V> propertyExpression) {
        Function<S, V> extractor = Expressions.compile(propertyExpression);
        return new PropertyExpressionValueProvider<S, T, V>() {
            @Override
            public PropertyExpression<S, T, V> property() {
                return propertyExpression;
            }

            @Override
            public V value(S object) {
                return extractor.apply(object);
            }
        };
    }
}
