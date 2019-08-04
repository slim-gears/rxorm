package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.NumericExpression;

@AutoValue
public abstract class NumericArgumentExpression<S, T extends Number & Comparable<T>> implements ArgumentExpression<S, T>, NumericExpression<S, T> {
    @JsonCreator
    public static <S, T extends Number & Comparable<T>> NumericArgumentExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<T> argType) {
        return new AutoValue_NumericArgumentExpression<>(type, argType);
    }
}
