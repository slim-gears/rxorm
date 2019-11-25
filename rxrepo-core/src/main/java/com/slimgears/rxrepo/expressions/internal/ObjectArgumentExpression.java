package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.reflect.TypeTokens;

@AutoValue
public abstract class ObjectArgumentExpression<S, T>
    extends AbstractArgumentExpression<S, T>
    implements ObjectExpression<S, T> {
    @JsonCreator
    public static <S, T> ObjectArgumentExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<T> argType) {
        return new AutoValue_ObjectArgumentExpression<>(type, TypeTokens.eliminateTypeVars(argType));
    }
}
