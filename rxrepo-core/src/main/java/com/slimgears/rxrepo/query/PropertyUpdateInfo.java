package com.slimgears.rxrepo.query;

import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;

@AutoValue
public abstract class PropertyUpdateInfo<S, T, V> {
    public abstract PropertyExpression<S, T, V> property();
    public abstract ObjectExpression<S, V> updater();

    public static <S, T, V> PropertyUpdateInfo<S, T, V> create(PropertyExpression<S, T, V> property, ObjectExpression<S, V> updater) {
        return new AutoValue_PropertyUpdateInfo<>(property, updater);
    }
}
