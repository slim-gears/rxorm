package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.CollectionOperationExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

@AutoValue
public abstract class MapCollectionOperationExpression<S, T, R, C extends Collection<T>> implements CollectionOperationExpression<S, T, R, R, C, Collection<R>> {
    @Override
    public TypeToken<Collection<R>> objectType() {
        return TypeToken.ofParameterized(Collection.class, operation().objectType());
    }

    @JsonCreator
    public static <S, T, R, C extends Collection<T>> MapCollectionOperationExpression<S, T, R, C> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, C> source,
            @JsonProperty("operation") ObjectExpression<T, R> operation) {
        return new AutoValue_MapCollectionOperationExpression<>(type, source, operation);
    }
}
