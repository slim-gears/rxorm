package com.slimgears.rxrepo.query.provider;

import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;

import java.util.Collection;

@AutoValue
public abstract class CollectionPropertyUpdateInfo<S, T, V, C extends Collection<V>> {
    public enum Operation {
        Add,
        Remove
    }

    public abstract CollectionPropertyExpression<S, T, V, C> property();
    public abstract ObjectExpression<S, V> item();
    public abstract Operation operation();

    public static <S, T, V, C extends Collection<V>> CollectionPropertyUpdateInfo<S, T, V, C> create(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> updater, Operation operation) {
        return new AutoValue_CollectionPropertyUpdateInfo<>(property, updater, operation);
    }
}
