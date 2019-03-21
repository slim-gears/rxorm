package com.slimgears.rxrepo.expressions;

import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanUnaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionArgumentExpression;
import com.slimgears.rxrepo.expressions.internal.FilterCollectionOperationExpression;
import com.slimgears.rxrepo.expressions.internal.FlatMapCollectionOperationExpression;
import com.slimgears.rxrepo.expressions.internal.MapCollectionOperationExpression;
import com.slimgears.rxrepo.expressions.internal.TypeTokens;
import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

public interface CollectionExpression<S, E> extends ObjectExpression<S, Collection<E>> {
    default BooleanExpression<S> contains(ObjectExpression<S, E> item) {
        return BooleanBinaryOperationExpression.create(Expression.Type.Contains, this, item);
    }

    default TypeToken<E> elementType() {
        return TypeTokens.element(objectType());
    }

    default BooleanExpression<S> contains(E item) {
        return contains(ConstantExpression.of(item));
    }

    default BooleanExpression<S> isEmpty() {
        return BooleanUnaryOperationExpression.create(Type.IsEmpty, this);
    }

    default BooleanExpression<S> isNotEmpty() {
        return isEmpty().not();
    }

    default <R> CollectionExpression<S, R> map(ObjectExpression<E, R> mapper) {
        return MapCollectionOperationExpression
                .create(Type.MapCollection, this, mapper);
    }

    default <R> CollectionExpression<S, R> flatMap(ObjectExpression<E, Collection<R>> mapper) {
        return FlatMapCollectionOperationExpression
                .create(Type.FlatMapCollection, this, mapper);
    }

    default CollectionExpression<S, E> filter(ObjectExpression<E, Boolean> filter) {
        return FilterCollectionOperationExpression
                .create(Type.FilterCollection, this, filter);
    }

    default BooleanExpression<S> any(ObjectExpression<E, Boolean> condition) {
        return filter(condition).isNotEmpty();
    }

    default BooleanExpression<S> all(ObjectExpression<E, Boolean> condition) {
        return filter(BooleanExpression.not(condition)).isEmpty();
    }

    default <R, OE extends UnaryOperationExpression<S, Collection<E>, R>> OE aggregate(Aggregator<S, E, R, OE> aggregator) {
        return aggregator.apply(this);
    }

    static <S> CollectionExpression<Collection<S>, S> arg(TypeToken<S> argType) {
        return CollectionArgumentExpression.create(Type.CollectionArgument, TypeToken.ofParameterized(Collection.class, argType));
    }

    static <S, T> CollectionExpression<S, T> indirectArg(TypeToken<? extends T> argType) {
        return CollectionArgumentExpression.create(Type.CollectionArgument, TypeToken.ofParameterized(Collection.class, argType));
    }
}
