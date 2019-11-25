package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.NumericExpression;

@AutoValue
public abstract class NumericConstantExpression<S, V extends Number & Comparable<V>>
    extends AbstractConstantExpression<S, V>
    implements NumericExpression<S, V> {
    @JsonCreator
    public static <S, V extends Number & Comparable<V>> NumericConstantExpression<S, V> create(
            @JsonProperty("type") Type type,
            @JsonProperty("value") V value) {
        return new AutoValue_NumericConstantExpression<>(type, value);
    }
}
