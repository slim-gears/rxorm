package com.slimgears.util.repository.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.ArgumentExpression;
import com.slimgears.util.repository.expressions.CollectionExpression;

import java.util.Collection;

@AutoValue
public abstract class CollectionArgumentExpression<S, T> implements ArgumentExpression<S, Collection<T>>, CollectionExpression<S, T> {
    @JsonCreator
    public static <S, T> CollectionArgumentExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<Collection<T>> argType) {
        return new AutoValue_CollectionArgumentExpression<>(type, argType);
    }
}
