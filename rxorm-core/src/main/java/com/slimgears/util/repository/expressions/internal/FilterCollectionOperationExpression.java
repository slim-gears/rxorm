package com.slimgears.util.repository.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.CollectionOperationExpression;
import com.slimgears.util.repository.expressions.ObjectExpression;

import java.util.Collection;

@AutoValue
public abstract class FilterCollectionOperationExpression<S, T> implements CollectionOperationExpression<S, T, Boolean, T> {
    @Override
    public TypeToken<? extends Collection<T>> objectType() {
        return source().objectType();
    }

    @JsonCreator
    public static <S, T> FilterCollectionOperationExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, Collection<T>> source,
            @JsonProperty("operation") ObjectExpression<T, Boolean> operation) {
        return new AutoValue_FilterCollectionOperationExpression<>(type, source, operation);
    }
}
