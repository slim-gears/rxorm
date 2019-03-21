package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.CollectionOperationExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import java.util.Collection;

@AutoValue
public abstract class FlatMapCollectionOperationExpression<S, T, R> implements CollectionOperationExpression<S, T, Collection<R>, R> {
    @Override
    public TypeToken<? extends Collection<R>> objectType() {
        return operation().objectType();
    }

    @JsonCreator
    public static <S, T, R> FlatMapCollectionOperationExpression<S, T, R> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, Collection<T>> source,
            @JsonProperty("operation") ObjectExpression<T, Collection<R>> operation) {
        return new AutoValue_FlatMapCollectionOperationExpression<>(type, source, operation);
    }
}
