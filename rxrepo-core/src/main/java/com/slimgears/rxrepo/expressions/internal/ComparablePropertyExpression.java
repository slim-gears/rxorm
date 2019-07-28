package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ComparableExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

@AutoValue
public abstract class ComparablePropertyExpression<S, T, V extends Comparable<V>> implements PropertyExpression<S, T, V>, ComparableExpression<S, V> {
    @JsonCreator
    public static <S, T, V extends Comparable<V>> ComparablePropertyExpression<S, T, V> create(
            @JsonProperty("type") Type type,
            @JsonProperty("target") ObjectExpression<S, T> target,
            @JsonProperty("property") PropertyMeta<T, V> property) {
        return new AutoValue_ComparablePropertyExpression<>(type, target, property);
    }
}
