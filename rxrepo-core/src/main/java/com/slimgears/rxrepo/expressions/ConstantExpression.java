package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.internal.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public interface ConstantExpression<S, T> extends ObjectExpression<S, T> {
    @Nullable @JsonProperty T value();

    @SuppressWarnings("unchecked")
    default TypeToken<T> objectType() {
        return TypeToken.of((Class<T>)value().getClass());
    }

    static <S, V> ConstantExpression<S, V> ofNull(TypeToken<V> type) {
        return NullConstantExpression.create(Type.NullConstant, type);
    }

    static <S, V> ConstantExpression<S, V> of(@Nonnull V value) {
        return ObjectConstantExpression.create(Type.Constant, value);
    }

    static <S, V extends Number & Comparable<V>> NumericConstantExpression<S, V> of(@Nonnull V value) {
        return NumericConstantExpression.create(Type.NumericConstant, value);
    }

    static <S> StringConstantExpression<S> of(@Nonnull String value) {
        return StringConstantExpression.create(Type.StringConstant, value);
    }

    static <S> BooleanConstantExpression<S> of(boolean value) {
        return BooleanConstantExpression.create(Type.BooleanConstant, value);
    }

    static <S, E, C extends Collection<E>> CollectionConstantExpression<S, E, C> of(@Nonnull C collection) {
        return CollectionConstantExpression.create(Type.CollectionConstant, collection);
    }

    @SafeVarargs
    static <S, E> CollectionConstantExpression<S, E, List<E>> of(E... items) {
        return of(Arrays.asList(items));
    }
}
