package com.slimgears.util.repository.query;

import com.google.auto.value.AutoValue;
import com.slimgears.util.autovalue.annotations.BuilderPrototype;
import com.slimgears.util.repository.expressions.ObjectExpression;
import com.slimgears.util.repository.expressions.PropertyExpression;

@AutoValue
public abstract class PropertyUpdateInfo<S, T, V> {
    public abstract PropertyExpression<S, T, V> property();
    public abstract ObjectExpression<S, V> updater();

    public static <S, T, V> PropertyUpdateInfo<S, T, V> create(PropertyExpression<S, T, V> property, ObjectExpression<S, V> updater) {
        return new AutoValue_PropertyUpdateInfo<>(property, updater);
    }
}
