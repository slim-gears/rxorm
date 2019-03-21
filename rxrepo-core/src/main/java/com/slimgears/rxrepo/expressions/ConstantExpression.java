package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slimgears.rxrepo.expressions.internal.BooleanConstantExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionConstantExpression;
import com.slimgears.rxrepo.expressions.internal.NumericConstantExpression;
import com.slimgears.rxrepo.expressions.internal.ObjectConstantExpression;
import com.slimgears.rxrepo.expressions.internal.StringConstantExpression;
import com.slimgears.util.reflect.TypeToken;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;

public interface ConstantExpression<S, T> extends ObjectExpression<S, T> {
    @JsonProperty T value();

    default TypeToken<T> objectType() {
        //noinspection unchecked
        return TypeToken.of((Class<T>)value().getClass());
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

    static <S, E> CollectionConstantExpression<S, E> of(@Nonnull Collection<E> collection) {
        return CollectionConstantExpression.create(Type.CollectionConstant, collection);
    }

    @SafeVarargs
    static <S, E> CollectionConstantExpression<S, E> of(E... items) {
        return of(Arrays.asList(items));
    }
}
