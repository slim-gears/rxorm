package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.NumericExpression;
import com.slimgears.rxrepo.expressions.StringExpression;
import com.slimgears.util.reflect.TypeToken;

@AutoValue
public abstract class StringArgumentExpression<S> implements ArgumentExpression<S, String>, StringExpression<S> {
    @JsonCreator
    public static <S> StringArgumentExpression<S> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<String> argType) {
        return new AutoValue_StringArgumentExpression<>(type, argType);
    }
}
