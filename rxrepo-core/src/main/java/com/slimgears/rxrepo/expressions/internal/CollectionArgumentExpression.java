package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.CollectionExpression;

import java.util.Collection;

@AutoValue
public abstract class CollectionArgumentExpression<S, T, C extends Collection<T>> implements ArgumentExpression<S, C>, CollectionExpression<S, T, C> {
    @JsonCreator
    public static <S, T, C extends Collection<T>> CollectionArgumentExpression<S, T, C> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<C> argType) {
        return new AutoValue_CollectionArgumentExpression<>(type, argType);
    }
}
