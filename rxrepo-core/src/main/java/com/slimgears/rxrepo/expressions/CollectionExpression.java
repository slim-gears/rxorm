package com.slimgears.rxrepo.expressions;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.internal.*;

import java.util.Collection;

public interface CollectionExpression<S, E, C extends Collection<E>> extends ObjectExpression<S, C> {
    default BooleanExpression<S> contains(ObjectExpression<S, E> item) {
        return BooleanBinaryOperationExpression.create(Expression.Type.Contains, this, item);
    }

    default BooleanExpression<S> contains(E item) {
        return contains(ConstantExpression.of(item));
    }

    default BooleanExpression<S> isEmpty() {
        return BooleanUnaryOperationExpression.create(Type.CollectionIsEmpty, this);
    }

    default BooleanExpression<S> isNotEmpty() {
        return isEmpty().not();
    }

    default NumericExpression<S, Integer> size() {
        return NumericUnaryOperationExpression.create(Type.CollectionSize, this);
    }

    default <R> CollectionExpression<S, R, Collection<R>> map(ObjectExpression<E, R> mapper) {
        return MapCollectionOperationExpression
                .create(Type.CollectionMap, this, mapper);
    }

    default <R> CollectionExpression<S, R, Collection<R>> flatMap(ObjectExpression<E, Collection<R>> mapper) {
        return FlatMapCollectionOperationExpression
                .create(Type.CollectionFlatMap, this, mapper);
    }

    default CollectionExpression<S, E, C> filter(ObjectExpression<E, Boolean> filter) {
        return FilterCollectionOperationExpression
                .create(Type.CollectionFilter, this, filter);
    }

    default BooleanExpression<S> any(ObjectExpression<E, Boolean> condition) {
        return filter(condition).isNotEmpty();
    }

    default BooleanExpression<S> all(ObjectExpression<E, Boolean> condition) {
        return filter(BooleanExpression.not(condition)).isEmpty();
    }

    default <R> UnaryOperationExpression<S, C, R> aggregate(Aggregator<S, E, R> aggregator) {
        return aggregator.apply(this);
    }

    static <S, C extends Collection<S>> CollectionExpression<C, S, C> arg(TypeToken<C> collectionType) {
        return CollectionArgumentExpression.create(Type.CollectionArgument, collectionType);
    }

    static <S, T, C extends Collection<T>> CollectionExpression<S, T, C> indirectArg(TypeToken<C> collectionType) {
        return CollectionArgumentExpression.create(Type.CollectionArgument, collectionType);
    }
}
