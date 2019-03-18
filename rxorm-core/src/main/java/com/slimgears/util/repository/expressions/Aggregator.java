package com.slimgears.util.repository.expressions;

import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.internal.ComparableUnaryOperationExpression;
import com.slimgears.util.repository.expressions.internal.NumericUnaryOperationExpression;

import java.util.Collection;

public interface Aggregator<S, T, R, E extends UnaryOperationExpression<S, Collection<T>, R>> {
    E apply(ObjectExpression<S, Collection<T>> collection);

    default TypeToken<? extends R> objectType(TypeToken<? extends T> element) {
        return apply(CollectionExpression.indirectArg(element)).objectType();
    }

    static <S, V> Aggregator<S, V, Long, NumericUnaryOperationExpression<S, Collection<V>, Long>> count() {
        return source -> NumericUnaryOperationExpression.create(Expression.Type.Count, source);
    }

    static <S, V extends Number & Comparable<V>> Aggregator<S, V, V, NumericUnaryOperationExpression<S, Collection<V>, V>> sum() {
        return source -> NumericUnaryOperationExpression.create(Expression.Type.Sum, source);
    }

    static <S, V extends Number & Comparable<V>> Aggregator<S, V, Double, NumericUnaryOperationExpression<S, Collection<V>, Double>> average() {
        return source -> NumericUnaryOperationExpression.create(Expression.Type.Average, source);
    }

    static <S, V extends Comparable<V>> Aggregator<S, V, V, ComparableUnaryOperationExpression<S, Collection<V>, V>> min() {
        return source -> ComparableUnaryOperationExpression.create(Expression.Type.Min, source);
    }

    static <S, V extends Comparable<V>> Aggregator<S, V, V, ComparableUnaryOperationExpression<S, Collection<V>, V>> max() {
        return source -> ComparableUnaryOperationExpression.create(Expression.Type.Max, source);
    }
}
