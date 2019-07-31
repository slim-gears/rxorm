package com.slimgears.rxrepo.expressions;

import com.slimgears.rxrepo.expressions.internal.ComparableUnaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.NumericUnaryOperationExpression;
import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

public interface Aggregator<S, T, R> {
    <C extends Collection<T>> UnaryOperationExpression<S, C, R> apply(ObjectExpression<S, C> collection);
    TypeToken<R> objectType(TypeToken<T> element);

    static <S, V> Aggregator<S, V, Long> count() {
        return new Aggregator<S, V, Long>() {
            @Override
            public <C extends Collection<V>> UnaryOperationExpression<S, C, Long> apply(ObjectExpression<S, C> collection) {
                return NumericUnaryOperationExpression.create(Expression.Type.Count, collection);
            }

            @Override
            public TypeToken<Long> objectType(TypeToken<V> element) {
                return TypeToken.of(Long.class);
            }
        };
    }

    static <S, V extends Number & Comparable<V>> Aggregator<S, V, V> sum() {
        return new Aggregator<S, V, V>() {
            @Override
            public <C extends Collection<V>> UnaryOperationExpression<S, C, V> apply(ObjectExpression<S, C> collection) {
                return NumericUnaryOperationExpression.create(Expression.Type.Sum, collection);
            }

            @Override
            public TypeToken<V> objectType(TypeToken<V> element) {
                return element;
            }
        };
    }

    static <S, V extends Number & Comparable<V>> Aggregator<S, V, Double> average() {
        return new Aggregator<S, V, Double>() {
            @Override
            public <C extends Collection<V>> UnaryOperationExpression<S, C, Double> apply(ObjectExpression<S, C> collection) {
                return NumericUnaryOperationExpression.create(Expression.Type.Average, collection);
            }

            @Override
            public TypeToken<Double> objectType(TypeToken<V> element) {
                return TypeToken.of(Double.class);
            }
        };
    }

    static <S, V extends Comparable<V>> Aggregator<S, V, V> min() {
        return new Aggregator<S, V, V>() {
            @Override
            public <C extends Collection<V>> UnaryOperationExpression<S, C, V> apply(ObjectExpression<S, C> collection) {
                return ComparableUnaryOperationExpression.create(Expression.Type.Min, collection);
            }

            @Override
            public TypeToken<V> objectType(TypeToken<V> element) {
                return element;
            }
        };
    }

    static <S, V extends Comparable<V>> Aggregator<S, V, V> max() {
        return new Aggregator<S, V, V>() {
            @Override
            public <C extends Collection<V>> UnaryOperationExpression<S, C, V> apply(ObjectExpression<S, C> collection) {
                return ComparableUnaryOperationExpression.create(Expression.Type.Max, collection);
            }

            @Override
            public TypeToken<V> objectType(TypeToken<V> element) {
                return element;
            }
        };
    }
}
