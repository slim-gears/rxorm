package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ConstantExpression;

import java.util.Collection;

@AutoValue
public abstract class CollectionConstantExpression<S, E, C extends Collection<E>> implements ConstantExpression<S, C>, CollectionExpression<S, E, C> {
    @JsonCreator
    public static <S, E, C extends Collection<E>> CollectionConstantExpression<S, E, C> create(@JsonProperty("type") Type type, @JsonProperty C value) {
        return new AutoValue_CollectionConstantExpression<>(type, value);
    }
}
