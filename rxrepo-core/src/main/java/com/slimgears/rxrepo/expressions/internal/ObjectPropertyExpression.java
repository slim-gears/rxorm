package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;

@AutoValue
public abstract class ObjectPropertyExpression<S, T, V> implements PropertyExpression<S, T, V> {
    @JsonCreator
    public static <S, T, V> ObjectPropertyExpression<S, T, V> create(
            @JsonProperty("type") Type type,
            @JsonProperty("target") ObjectExpression<S, T> target,
            @JsonProperty("property") PropertyMeta<T, ? extends V> property) {
        return new AutoValue_ObjectPropertyExpression<>(type, target, property);
    }
}
