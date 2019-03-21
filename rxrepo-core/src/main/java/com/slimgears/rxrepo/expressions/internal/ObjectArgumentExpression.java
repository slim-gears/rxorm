package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ObjectExpression;

@AutoValue
public abstract class ObjectArgumentExpression<S, T> implements ArgumentExpression<S, T>, ObjectExpression<S, T> {
    @JsonCreator
    public static <S, T> ObjectArgumentExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<T> argType) {
        //noinspection unchecked
        return new AutoValue_ObjectArgumentExpression<>(type, (TypeToken<T>)argType.eliminateTypeVars());
    }
}
