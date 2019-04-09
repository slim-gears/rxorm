package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.StringExpression;
import com.slimgears.util.reflect.TypeToken;

@AutoValue
public abstract class BooleanArgumentExpression<S> implements ArgumentExpression<S, Boolean>, BooleanExpression<S> {
    @JsonCreator
    public static <S> BooleanArgumentExpression<S> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<Boolean> argType) {
        return new AutoValue_BooleanArgumentExpression<>(type, argType);
    }
}
