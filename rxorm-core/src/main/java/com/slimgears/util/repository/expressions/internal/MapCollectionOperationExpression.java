package com.slimgears.util.repository.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.CollectionExpression;
import com.slimgears.util.repository.expressions.CollectionOperationExpression;
import com.slimgears.util.repository.expressions.ObjectExpression;

import java.util.Collection;

@AutoValue
public abstract class MapCollectionOperationExpression<S, T, R> implements CollectionOperationExpression<S, T, R, R> {
    @Override
    public TypeToken<? extends Collection<R>> objectType() {
        return TypeToken.ofParameterized(Collection.class, operation().objectType());
    }

    @JsonCreator
    public static <S, T, R> MapCollectionOperationExpression<S, T, R> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, Collection<T>> source,
            @JsonProperty("operation") ObjectExpression<T, R> operation) {
        return new AutoValue_MapCollectionOperationExpression<>(type, source, operation);
    }
}
